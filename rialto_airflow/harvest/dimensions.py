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

                # dimensions doesn't want scheme and host
                orcid_query_str = normalize_orcid(author.orcid)
                for dimensions_pub_json in orcid_publications(orcid_query_str):
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


def orcid_publications(orcid: str) -> Generator[dict, None, None]:
    dois = dois_from_orcid(orcid)
    yield from publications_from_dois(dois)


def dois_from_orcid(orcid):
    logging.info(f"looking up dois for orcid {orcid}")
    q = """
        search publications where researchers.orcid_id = "{}"
        return publications [doi]
        limit 1000
        """.format(orcid)

    result = query_with_retry(q, 20)

    if len(result["publications"]) == 1000:
        logging.warning("Truncated results for ORCID %s", orcid)
    for pub in result["publications"]:
        if pub.get("doi"):
            doi_id = normalize_doi(pub["doi"])
            yield doi_id


def publications_from_dois(dois: list, batch_size=200):
    """
    Get the publications metadata for the provided list of DOIs.
    """
    fields = " + ".join(publication_fields())
    for doi_batch in batched(dois, batch_size):
        doi_list = ",".join(['"{}"'.format(doi) for doi in doi_batch])

        q = f"""
            search publications where doi in [{doi_list}]
            return publications [{fields}]
            limit 1000
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
    return list(result.data["sources"]["publications"]["fields"].keys())


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
        os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER"),
        os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS"),
        "https://app.dimensions.ai",
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

        try:
            # dimcli will retry HTTP level errors, but not ones involving the connection
            return dsl().query(q, retry=retry)
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
