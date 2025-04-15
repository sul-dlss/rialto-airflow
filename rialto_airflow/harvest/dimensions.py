import json
import logging
import os
import time
from functools import cache
from pathlib import Path
from typing import Generator

import dimcli
import requests
from more_itertools import batched
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import (
    Author,
    Publication,
    get_session,
    pub_author_association,
)
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi, normalize_orcid


def harvest(snapshot: Snapshot, limit: None | int = None) -> Path:
    """
    Walk through all the Author ORCIDs and generate publications for them.
    """
    jsonl_file = snapshot.path / "dimensions.jsonl"
    count = 0
    stop = False

    with jsonl_file.open("w") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            # get all authors that have an ORCID
            for author in (
                select_session.query(Author).where(Author.orcid.is_not(None)).all()  # type: ignore
            ):
                if stop is True:
                    logging.info(f"Reached limit of {limit} publications stopping")
                    break

                for dimensions_pub_json in publications_from_orcid(author.orcid):
                    count += 1
                    if limit is not None and count > limit:
                        stop = True
                        break

                    doi = normalize_doi(dimensions_pub_json.get("doi", None))
                    with get_session(snapshot.database_name).begin() as insert_session:
                        # if there's a DOI constraint violation, update the existing row's JSON
                        pub_id = insert_session.execute(
                            insert(Publication)
                            .values(doi=doi, dim_json=dimensions_pub_json)
                            .on_conflict_do_update(
                                constraint="publication_doi_key",
                                set_=dict(dim_json=dimensions_pub_json),
                            )
                            .returning(Publication.id)
                        ).scalar_one()

                        # a constraint violation here means we already know the
                        # publication is by the author
                        insert_session.execute(
                            insert(pub_author_association)
                            .values(publication_id=pub_id, author_id=author.id)
                            .on_conflict_do_nothing()
                        )

                        jsonl_output.write(json.dumps(dimensions_pub_json) + "\n")

    return jsonl_file


def publications_from_dois(dois: list, batch_size=200):
    """
    Get the publications metadata for the provided list of DOIs.
    """
    fields = " + ".join(publication_fields())
    for doi_batch in batched(dois, batch_size):
        doi_list = ",".join(['"{}"'.format(doi) for doi in doi_batch])
        logging.info(f"looking up: {doi_list}")

        q = f"""
            search publications where doi in [{doi_list}]
            return publications [{fields}]
            """

        result = query_with_retry(q, retry=5)
        for pub in result["publications"]:
            yield normalize_publication(pub)


def publications_from_orcid(orcid: str, batch_size=200):
    """
    Get the publications metadata for a given ORCID.
    """
    logging.info(f"looking up publications for orcid {orcid}")
    orcid = normalize_orcid(orcid)
    fields = " + ".join(publication_fields())

    q = f"""
        search publications where researchers.orcid_id = "{orcid}"
        return publications [{fields}]
        """

    result = query_with_retry(q, retry=5)
    for pub in result["publications"]:
        yield normalize_publication(pub)


@cache
def publication_fields():
    """
    Get a list of all possible fields for publications.
    """
    result = dsl().query("describe schema")
    fields = list(result.data["sources"]["publications"]["fields"].keys())

    # for some reason "researchers" causes 408 errors when harvesting, maybe we
    # can add it back when this is resolved?
    fields.remove("researchers")

    return fields


def normalize_publication(pub) -> dict:
    for field in publication_fields():
        if field not in pub:
            pub[field] = None

    return pub


@cache  # TODO: maybe the login should expire after some time?
def login():
    """
    Login to Dimensions API and cache the result.
    """
    dimcli.login(
        key=os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_KEY"),
        endpoint="https://app.dimensions.ai/api/dsl/v2",
    )


def dsl():
    """
    Get the Dimensions DSL for querying.
    """
    login()
    return dimcli.Dsl(verbose=False)


def query_with_retry(q, retry=5):
    # Catch all request exceptions since dimcli currently only catches HTTP exceptions
    # see: https://github.com/digital-science/dimcli/issues/88
    try_count = 0
    while True:
        try_count += 1

        # dimcli will retry HTTP level errors, but not ones involving the connection
        try:
            # use query_iterative which will page responses but aggregate them
            # into a complete result set. The maximum number of results is 50,000.
            return dsl().query_iterative(q, show_results=False)
        except requests.exceptions.RequestException as e:
            if try_count > retry:
                logging.error(
                    "Dimensions API call %s resulted in error: %s", try_count, e
                )
                raise e
            else:
                logging.warning(
                    "Dimensions query error retry %s of %s: %s", try_count, retry, e
                )
                time.sleep(try_count * 10)


def fill_in(snapshot: Snapshot, jsonl_file: Path) -> Path:
    """Harvest Dimensions data for DOIs from other publication sources."""
    count = 0
    with jsonl_file.open("a") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            stmt = (
                select(Publication.doi)  # type: ignore
                .where(Publication.doi.is_not(None))  # type: ignore
                .where(Publication.dim_json.is_(None))
                .execution_options(yield_per=100)
            )

            for rows in select_session.execute(stmt).partitions():
                dois = [row["doi"] for row in rows]

                # note: we could potentially adjust batch_size upwards if we
                # want to look up more DOIs at a time
                for dimensions_pub in publications_from_dois(dois, batch_size=100):
                    doi = dimensions_pub.get("doi")
                    if doi is None:
                        logging.warning("unable to determine what DOI to update")
                        continue

                    with get_session(snapshot.database_name).begin() as update_session:
                        update_stmt = (
                            update(Publication)  # type: ignore
                            .where(Publication.doi == doi)
                            .values(dim_json=dimensions_pub)
                        )
                        update_session.execute(update_stmt)

                    count += 1
                    jsonl_output.write(json.dumps(dimensions_pub) + "\n")

    logging.info(f"filled in {count} publications")

    return snapshot.path
