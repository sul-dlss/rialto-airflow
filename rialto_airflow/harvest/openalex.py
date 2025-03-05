import json
import logging
import os
import time
from pathlib import Path

import requests
from pyalex import Authors, Works, config
from typing import Generator
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import (
    Author,
    Publication,
    get_session,
    pub_author_association,
)
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi

config.email = os.environ.get("AIRFLOW_VAR_OPENALEX_EMAIL")
config.max_retries = 5
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


def harvest(snapshot: Snapshot, limit=None) -> Path:
    """
    Walk through all the Author ORCIDs and generate publications for them.
    """
    jsonl_file = snapshot.path / "openalex.jsonl"
    count = 0
    stop = False

    with jsonl_file.open("w") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            # get all authors that have an ORCID
            # TODO: should we just pull the relevant bits back into memory since
            # that's what's going on with our client-side buffering connection
            # and there aren't that many of them?
            for author in (
                select_session.query(Author).where(Author.orcid.is_not(None)).all()  # type: ignore
            ):
                if stop is True:
                    logging.info(f"Reached limit of {limit} publications stopping")
                    break

                for openalex_pub in orcid_publications(author.orcid):
                    count += 1
                    if limit is not None and count > limit:
                        stop = True
                        break

                    doi = normalize_doi(openalex_pub.get("doi", None))

                    with get_session(snapshot.database_name).begin() as insert_session:
                        # if there's a DOI constraint violation we need to update instead of insert
                        pub_id = insert_session.execute(
                            insert(Publication)
                            .values(
                                doi=doi,
                                openalex_json=openalex_pub,
                            )
                            .on_conflict_do_update(
                                constraint="publication_doi_key",
                                set_=dict(openalex_json=openalex_pub),
                            )
                            .returning(Publication.id)
                        ).scalar_one()

                        # a constraint violation is ok here, since it means we
                        # already know that the publication is by the author
                        insert_session.execute(
                            insert(pub_author_association)
                            .values(publication_id=pub_id, author_id=author.id)
                            .on_conflict_do_nothing()
                        )

                        jsonl_output.write(json.dumps(openalex_pub) + "\n")

    return jsonl_file


def orcid_publications(orcid: str) -> Generator[dict, None, None]:
    """
    Pass in the ORCID ID and get an iterator for publications by that author.
    """
    # TODO: I think we can maybe have this function take a list of orcids and
    # batch process them since we can filter by multiple orcids in one request?
    logging.info(f"looking up dois for orcid {orcid}")

    # get the first (and hopefully only) openalex id for the orcid
    authors = Authors().filter(orcid=orcid).get()
    if len(authors) == 0:
        return
    elif len(authors) > 1:
        logging.warning(f"found more than one openalex author id for {orcid}")
    author_id = authors[0]["id"]

    # get all the works for the openalex author id
    for page in Works().filter(author={"id": author_id}).paginate(per_page=200):
        # TODO: get a key so we don't have to sleep!
        time.sleep(1)

        yield from page


def fill_in(snapshot: Snapshot, jsonl_file: Path) -> Path:
    """Harvest OpenAlex data for DOIs from other publication sources."""
    count = 0
    with jsonl_file.open("a") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            stmt = (
                select(Publication.doi)
                .where(Publication.doi.is_not(None))
                .where(Publication.openalex_json.is_(None))
                .execution_options(yield_per=100)
            )
            for row in select_session.execute(stmt):
                logging.info(f"filling in data for {row.doi}")
                try:
                    openalex_pub = Works()[f"https://doi.org/{row.doi}"]
                    # TODO: get a key so we don't have to sleep!
                    time.sleep(1)
                except requests.exceptions.HTTPError as e:
                    logging.error(f"error looking up {row.doi}: {e}")
                    continue

                with get_session(snapshot.database_name).begin() as update_session:
                    update_stmt = (
                        update(Publication)
                        .where(Publication.doi == row.doi)
                        .values(openalex_json=openalex_pub)
                    )
                    update_session.execute(update_stmt)

                count += 1
                jsonl_output.write(json.dumps(openalex_pub) + "\n")

    logging.info(f"filled in {count} publications")

    return snapshot
