import json
import logging
import os
import re
from itertools import batched
from pathlib import Path

import requests
from typing import Generator, Optional, Dict, Union
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


def fill_in(snapshot: Snapshot, jsonl_file: Path):
    """Harvest WebOfScience data for DOIs from other publication sources."""
    count = 0
    with jsonl_file.open("a") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            stmt = (
                select(Publication.doi)  # type: ignore
                .where(Publication.doi.is_not(None))  # type: ignore
                .where(Publication.wos_json.is_(None))
                .execution_options(yield_per=50)
            )

            for rows in select_session.execute(stmt).partitions():
                # since the query uses yield_per=50 we will be looking up 50 DOIs at a time
                dois = [row["doi"] for row in rows]

                logging.info(f"looking up DOIs {dois}")
                for wos_pub in publications_from_dois(dois):
                    doi = normalize_doi(wos_pub.get("doi"))
                    if doi is None:
                        continue

                    with get_session(snapshot.database_name).begin() as update_session:
                        update_stmt = (
                            update(Publication)  # type: ignore
                            .where(Publication.doi == doi)
                            .values(wos_json=wos_pub)
                        )
                        update_session.execute(update_stmt)

                    count += 1
                    jsonl_output.write(json.dumps(wos_pub) + "\n")

    logging.info(f"filled in {count} publications")

    return snapshot.path


def orcid_publications(orcid) -> Generator[dict, None, None]:
    """
    A generator that returns publications associated with a given ORCID.
    """
    # WoS doesn't recognize ORCID URIs which are stored in User table
    if m := re.match(r"^https?://orcid.org/(.+)$", orcid):
        orcid = m.group(1)

    yield from _wos_api(f"AI={orcid}")


def publications_from_dois(dois: list[str]) -> Generator[dict, None, None]:
    """
    A generator that returns publications associated a list of DOIs.
    """
    for doi_batch in batched(dois, n=50):
        yield from _wos_api(f"DO=({' '.join(doi_batch)})")


def _wos_api(query) -> Generator[dict, None, None]:
    """
    A generator that returns publications associated a list of DOIs.
    """

    # For API details see: https://api.clarivate.com/swagger-ui/?apikey=none&url=https%3A%2F%2Fdeveloper.clarivate.com%2Fapis%2Fwos%2Fswagger

    wos_key = os.environ.get("AIRFLOW_VAR_WOS_KEY")
    base_url = "https://wos-api.clarivate.com/api/wos"
    headers = {"Accept": "application/json", "X-ApiKey": wos_key}

    # the number of records to get in each request (100 is max)
    count = 100

    params: Params = {
        "databaseId": "WOK",
        "usrQuery": query,
        "count": count,
        "firstRecord": 1,
        "optionView": "SR",  # SR = Short Records, which gives us the most basic info about the publication, skipping authors, to keep
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
        logging.info(f"No results found for {query}")
        return

    yield from results["Data"]["Records"]["records"]["REC"]

    # get subsequent results using the Query ID

    query_id = results["QueryResult"]["QueryID"]
    records_found = results["QueryResult"]["RecordsFound"]
    first_record = count + 1  # since the initial set included 100

    # if there aren't any more results to fetch this loop will never be entered

    logging.info(f"{records_found} records found")
    while first_record < records_found:
        page_params: Params = {"firstRecord": first_record, "count": count}
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
        first_record += count


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


def check_status(resp: requests.Response) -> bool:
    # see https://github.com/sul-dlss/rialto-airflow/issues/208
    if (
        resp.status_code == 500
        and resp.headers.get("Content-Type") == "application/json"
        and "Customization error" in resp.json().get("message", "")
    ):
        # TODO: this would be a good place for a honeybadger alert, so missing data doesn't fall through the cracks
        logging.error(f"got a 500 Customization Error when looking up {resp.url}")
        return False
    else:
        # try raise_for_status() is pretty much what requests.Response.ok() does. But
        # doing similar, instead of a conditional on the ok() result, allows us to leverage
        # the error message building that raise_for_status() already does.
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # TODO: this would be a good place for a honeybadger alert, so missing data doesn't fall through the cracks
            logging.error(f"{e} -- {resp.text}")
            return False
        return True


def get_doi(pub) -> Optional[str]:
    try:
        identifiers_field = (
            pub.get("dynamic_data", {})
            .get("cluster_related", {})
            .get("identifiers", {})
        )

        if isinstance(identifiers_field, dict):
            # The "identifier" field (from "identifiers" above) is usually a list. But sometimes
            # there is just one string value instead. (as an examle, see record for WOS:000299597104419)
            ids = identifiers_field.get("identifier", [])
            ids = [ids] if isinstance(ids, dict) else ids

            for id in ids:
                if id["type"] == "doi":
                    return normalize_doi(id["value"])
        elif isinstance(identifiers_field, str):
            # We have seen at least one publication, WOS:000089165000013, where the "identifiers" field is
            # a str instead of a dict, albeit an empty string in that case. Normalize empty string to None.
            return identifiers_field or None
    except AttributeError as e:
        logging.warning(f"error {e} trying to parse identifiers from {pub}")
        return None

    logging.warning(f"unable to determine what DOI to update: {pub}")
    return None
