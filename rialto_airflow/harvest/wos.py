import json
import logging
import os
import re
from pathlib import Path

import requests
from typing import Generator, Optional, Dict, Union
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import (
    Author,
    Publication,
    get_session,
    pub_author_association,
)
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi

Params = Dict[str, Union[int, str]]


def harvest(snapshot: Snapshot, limit=None) -> Path:
    """
    Walk through all the Author ORCIDs and generate publications for them.
    """
    jsonl_file = snapshot.path / "wos.jsonl"
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

                for wos_pub in orcid_publications(author.orcid):
                    count += 1
                    if limit is not None and count > limit:
                        stop = True
                        break

                    doi = get_doi(wos_pub)

                    with get_session(snapshot.database_name).begin() as insert_session:
                        # if there's a DOI constraint violation we need to update instead of insert
                        pub_id = insert_session.execute(
                            insert(Publication)
                            .values(
                                doi=doi,
                                wos_json=wos_pub,
                            )
                            .on_conflict_do_update(
                                constraint="publication_doi_key",
                                set_=dict(wos_json=wos_pub),
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

                        jsonl_output.write(json.dumps(wos_pub) + "\n")

    return jsonl_file


def orcid_publications(orcid) -> Generator[dict, None, None]:
    """
    A generator that returns publications associated with a given ORCID.
    """

    # For API details see: https://api.clarivate.com/swagger-ui/

    # WoS doesn't recognize ORCID URIs which are stored in User table
    if m := re.match(r"^https?://orcid.org/(.+)$", orcid):
        orcid = m.group(1)

    wos_key = os.environ.get("AIRFLOW_VAR_WOS_KEY")
    base_url = "https://wos-api.clarivate.com/api/wos"
    headers = {"Accept": "application/json", "X-ApiKey": wos_key}

    # the number of records to get in each request (100 is max)
    batch_size = 100

    params: Params = {
        "databaseId": "WOK",
        "usrQuery": f"AI=({orcid})",
        "count": batch_size,
        "firstRecord": 1,
    }

    http = requests.Session()

    # get the initial set of results, which also gives us a Query ID to fetch
    # subsequent pages of results if there are any

    logging.info(f"fetching {base_url} with {params}")
    resp: requests.Response = http.get(base_url, params=params, headers=headers)

    if not check_status(resp):
        return

    results = get_json(resp)
    if results is None:
        return

    if results["QueryResult"]["RecordsFound"] == 0:
        logging.info(f"No results found for ORCID {orcid}")
        return

    yield from results["Data"]["Records"]["records"]["REC"]

    # get subsequent results using the Query ID

    query_id = results["QueryResult"]["QueryID"]
    records_found = results["QueryResult"]["RecordsFound"]
    first_record = batch_size + 1  # since the initial set included 100

    # if there aren't any more results to fetch this loop will never be entered

    logging.info(f"{records_found} records found")
    while first_record < records_found:
        page_params: Params = {"firstRecord": first_record, "count": batch_size}
        logging.info(f"fetching {base_url}/query/{query_id} with {page_params}")

        resp = http.get(
            f"{base_url}/query/{query_id}", params=page_params, headers=headers
        )

        if not check_status(resp):
            return

        records = get_json(resp)
        if records is None:
            break

        yield from records["Records"]["records"]["REC"]

        # move the offset along in the results list
        first_record += batch_size


def get_json(resp: requests.Response) -> Optional[dict]:
    try:
        return resp.json()
    except requests.exceptions.JSONDecodeError as e:
        # see https://github.com/sul-dlss/rialto-airflow/issues/207 for why
        if resp.text == "":
            logging.error(
                f"got empty string instead of JSON when looking up {resp.url}"
            )
            return None
        else:
            logging.error(f"uhoh, instead of JSON we got: {resp.text}")
            raise e


def check_status(resp):
    # see https://github.com/sul-dlss/rialto-airflow/issues/208
    if (
        resp.status_code == 500
        and resp.headers.get("Content-Type") == "application/json"
        and "Customization error" in resp.json().get("message", "")
    ):
        logging.error(f"got a 500 Customization Error when looking up {resp.url}")
        return False
    else:
        resp.raise_for_status()
        return True


def get_doi(pub) -> Optional[str]:
    ids = (
        pub.get("dynamic_data", {})
        .get("cluster_related", {})
        .get("identifiers", {})
        .get("identifier", [])
    )

    # sometimes there is just one id instead of a list of ids
    # as an examle see record for WOS:000299597104419
    if isinstance(ids, dict):
        ids = [ids]

    for id in ids:
        if id["type"] == "doi":
            return normalize_doi(id["value"])

    return None
