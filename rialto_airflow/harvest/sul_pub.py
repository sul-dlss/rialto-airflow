import json
import logging

import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from urllib3.util import Retry

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Author,
    Publication,
    pub_author_association,
)
from rialto_airflow.utils import normalize_doi


def harvest(snapshot, host, key, per_page=1000, limit=None):
    # create a jsonl file to write to
    jsonl_file = snapshot.path / "sulpub.jsonl"

    with jsonl_file.open("w") as jsonl_output:
        for sulpub_pub in publications(host, key, per_page, limit):
            with get_session(snapshot.database_name).begin() as session:
                doi = extract_doi(sulpub_pub)

                # only write approved publications to the database
                if approved(sulpub_pub):
                    # if when inserting the row we get a constraint violation
                    # on the DOI then we have to do an update
                    pub_id = session.execute(
                        insert(Publication)
                        .values(doi=doi, sulpub_json=sulpub_pub)
                        .on_conflict_do_update(
                            constraint="publication_doi_key",
                            set_=dict(sulpub_json=sulpub_pub),
                        )
                        .returning(Publication.id)
                    ).scalar_one()

                    # get a list of approved cap_ids for the publication
                    # the cap_profile_id needs to be a string for the database
                    cap_ids = [
                        str(a["cap_profile_id"])
                        for a in sulpub_pub["authorship"]
                        if a["status"] == "approved"
                    ]

                    # if the row is already there we could get a constraint
                    # violation, but we can safely ignore that
                    session.execute(
                        insert(pub_author_association)
                        .from_select(
                            ["publication_id", "author_id"],
                            select(pub_id, Author.id).where(
                                Author.cap_profile_id.in_(cap_ids)
                            ),
                        )
                        .on_conflict_do_nothing()
                    )

                jsonl_output.write(json.dumps(sulpub_pub) + "\n")

    return jsonl_file


def publications(host, key, per_page=1000, limit=None):
    url = f"https://{host}/publications.json"

    http_headers = {"CAPKEY": key}
    params = {"per": per_page}

    page = 0
    record_count = 0
    more = True

    # catch and back off from temporary errors from sulpub
    # https://docs.python-requests.org/en/latest/user/advanced/#example-automatic-retries

    http = requests.Session()
    retries = Retry(connect=5, read=5)
    http.mount("https://", HTTPAdapter(max_retries=retries))

    while more:
        page += 1
        params["page"] = page

        logging.info(f"fetching sul_pub results {url} {params}")
        resp = http.get(url, params=params, headers=http_headers)
        resp.raise_for_status()

        records = resp.json()["records"]
        if len(records) == 0:
            more = False

        for record in records:
            record_count += 1
            if limit is not None and record_count > limit:
                logging.warning(f"stopping with limit={limit}")
                more = False
                break

            yield record


def extract_doi(pub):
    if pub.get("doi"):
        return normalize_doi(pub["doi"])

    for id in pub.get("identifier"):
        if id.get("type") == "doi" and "id" in id:
            logging.debug(
                f"doi was not available in top level for sulpub id {pub.get('sulpubid')} but found in identifier block"
            )
            return normalize_doi(id["id"])
    return None


def approved(pub):
    """
    Returns True if at least one author has approved the publication, and False if not.
    """
    for authorship in pub["authorship"]:
        if authorship["status"] == "approved":
            return True
    return False
