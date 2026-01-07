import json
import logging
import os
import time
from functools import cache
from pathlib import Path

import dimcli
import requests
from more_itertools import batched
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Author,
    Publication,
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
                    logging.warning(f"Reached limit of {limit} publications stopping")
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
        logging.debug(f"looking up: {doi_list}")

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
    orcid = normalize_orcid(orcid)
    fields = " + ".join(publication_fields())
    logging.info(f"Harvesting publications for ORCID {orcid}")
    q = f"""
        search publications where researchers.orcid_id = "{orcid}"
        return publications[{fields}]
        """

    result = query_with_retry(q, retry=5)
    for pub in result["publications"]:
        yield normalize_publication(pub)


def publication_fields():
    # See Dimensions docs for a description of what is included in the "basics" and "extras" fieldsets:
    # https://docs.dimensions.ai/dsl/datasource-publications.html#publications-fieldsets
    return [
        "basics",
        "book",
        "extras",
        "abstract",
        "altmetric_id",
        "issn",
        "isbn",
        "publisher",
        "recent_citations",
        "supporting_grant_ids",
    ]


def unpacked_pub_fields():
    return [
        # basics
        "authors",
        "id",
        "issue",
        "journal",
        "pages",
        "title",
        "type",
        "volume",
        "year",
        # book
        "book_doi",
        "book_series_title",
        "book_title",
        # extras
        "altmetric",
        "date",
        "doi",
        "funders",
        "open_access",
        "pmcid",
        "pmid",
        "relative_citation_ratio",
        "research_org_cities",
        "research_org_countries",
        "research_org_country_names",
        "research_org_state_codes",
        "research_org_state_names",
        "research_orgs",
        "researchers",
        "times_cited",
        # specific fields
        "abstract",
        "altmetric_id",
        "issn",
        "isbn",
        "publisher",
        "recent_citations",
        "supporting_grant_ids",
    ]


def normalize_publication(pub) -> dict:
    for field in unpacked_pub_fields():
        if field not in pub:
            pub[field] = None

    return pub


@cache
def login():
    """
    Login to Dimensions API and cache the result.
    """
    logging.info("logging in to Dimensions API")
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
            # Using a limit param set to 15 because some recent results are very large and
            # we were getting an error if the response exceeds a certain size. 25 was proving
            # too high for some ORCIDs. Consider raising or removing the limit if the issue is resolved and
            # removing the force param to view errors.
            return dsl().query_iterative(q, show_results=False, limit=15, force=True)
        except requests.exceptions.RequestException as e:
            if try_count > retry:
                logging.error(
                    "Dimensions API call %s resulted in error: %s", try_count, e
                )
                raise e
            else:
                if (
                    e.response is not None and e.response.status_code == 401
                ):  # Response could be None
                    # Likely the token expired, clear cache. login() will be retried on next dsl() call
                    login.cache_clear()
                logging.warning(
                    "Dimensions query error retry %s of %s: %s", try_count, retry, e
                )
                time.sleep(try_count * 10)


def fill_in(snapshot: Snapshot):
    """Harvest Dimensions data for DOIs from other publication sources."""
    jsonl_file = snapshot.path / "dimensions.jsonl"
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
                dois = [row.doi for row in rows]

                # note: we could potentially adjust batch_size upwards if we
                # want to look up more DOIs at a time
                for dimensions_pub in publications_from_dois(dois, batch_size=100):
                    doi = dimensions_pub.get("doi")
                    if doi is None:
                        logging.warning(
                            f"unable to determine what DOI to update from {dimensions_pub}"
                        )
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

    return jsonl_file
