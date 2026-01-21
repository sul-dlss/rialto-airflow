import json
import logging
import os
import re
from itertools import batched
from pathlib import Path
from time import sleep

import requests
from requests.adapters import HTTPAdapter
from typing import Generator, Optional, Dict, Union
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from urllib3.util import Retry

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Author,
    Publication,
    pub_author_association,
)
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi, add_orcid

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
                    logging.warning(f"Reached limit of {limit} publications stopping")
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

                        jsonl_output.write(
                            json.dumps(add_orcid(wos_pub, author.orcid)) + "\n"
                        )

    return jsonl_file


def fill_in(snapshot: Snapshot):
    """Harvest WebOfScience data for DOIs from other publication sources."""
    jsonl_file = snapshot.path / "wos-fillin.jsonl"
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
                dois = [row.doi for row in rows]

                logging.debug(f"looking up DOIs {dois}")
                for wos_pub in publications_from_dois(dois):
                    doi = normalize_doi(get_doi(wos_pub))
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

    return jsonl_file


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

    It will first try to lookup the DOIs in batches.  If a batch fails it will
    try to look up each DOI in the batch individually, because we've seen batches
    fail due to a single DOI within it, where many of the others might lookup successfully.
    See https://github.com/sul-dlss/rialto-airflow/issues/680

    A wordy note on timeouts in the API calls below (first value is
    connection timeout, second is read):
    * per the docs -- https://requests.readthedocs.io/en/stable/user/advanced/#timeouts
    "It’s a good practice to set connect timeouts to slightly larger than a
    multiple of 3, which is the default TCP packet retransmission window."
    * The initial lookup read timeout is longer, a minute, but not too long, because responses should
    be relatively quick, and we have a lot of batches to get through.  The read timeout for individual
    lookups is much more aggressive at 15 seconds, because we switched to batches due to long runs
    as a result of individual DOI lookups.
    """
    for doi_batch in batched(dois, n=50):
        try:
            yield from _wos_api(
                f"DO=({' '.join(f'"{doi}"' for doi in doi_batch)})",
                should_raise_for_status=True,
                timeout=(6.05, 60),
            )
        except Exception as e:
            logging.error(
                f"Unexpected error querying for DOIs in DOI batch, trying one at a time.  doi_batch={doi_batch} -- error={e}"
            )
            for doi in doi_batch:
                try:
                    yield from _wos_api(f'DO=("{doi}")', timeout=(6.05, 15))
                except Exception as e:
                    logging.error(
                        f'Unexpected error querying for single DOI from larger batch.  DOI="{doi}" -- error={e}'
                    )


def _wos_api_retry() -> Retry:
    # Retry on HTTP 429.  We want to make sure we really back off, since status code 429
    # indicates we're being rate limited.
    # Retry up to 10 times on status code errors.
    #
    # For each try the time to sleep will be set using:
    # {backoff_factor} * (2 ** ({number of previous retries})) + random.uniform(0, {backoff_jitter})
    return Retry(
        status=10, status_forcelist=[429], backoff_factor=0.3, backoff_jitter=5
    )


def _wos_api(
    query,
    should_raise_for_status: bool = False,
    timeout: int | tuple[float, float] | None = None,
) -> Generator[dict, None, None]:
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

    # retry any 429 statuses and stay within rate limits
    http = requests.Session()
    http.mount("https://", HTTPAdapter(max_retries=_wos_api_retry()))

    # get the initial set of results, which also gives us a Query ID to fetch
    # subsequent pages of results if there are any
    logging.debug(f"fetching {base_url} with {params}")
    resp: requests.Response = http.get(
        base_url, params=params, headers=headers, timeout=timeout
    )
    if not check_status(resp, should_raise_for_status):
        return

    results = get_json(resp)
    if results is None:
        return

    if results["QueryResult"]["RecordsFound"] == 0:
        logging.debug(f"No results found for {query}")
        return

    yield from results["Data"]["Records"]["records"]["REC"]

    # get subsequent results using the Query ID

    query_id = results["QueryResult"]["QueryID"]
    records_found = results["QueryResult"]["RecordsFound"]
    first_record = count + 1  # since the initial set included 100

    # if there aren't any more results to fetch this loop will never be entered

    logging.debug(f"{records_found} records found")
    while first_record < records_found:
        sleep(0.5)
        page_params: Params = {"firstRecord": first_record, "count": count}
        logging.debug(f"fetching {base_url}/query/{query_id} with {page_params}")

        # retry any 429 errors and stay within rate limits
        http = requests.Session()
        http.mount("https://", HTTPAdapter(max_retries=_wos_api_retry()))
        resp = http.get(
            f"{base_url}/query/{query_id}",
            params=page_params,
            headers=headers,
            timeout=timeout,
        )
        if not check_status(resp, should_raise_for_status):
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


def check_status(resp: requests.Response, should_raise_for_status: bool) -> bool:
    try:
        # try raise_for_status() is pretty much what requests.Response.ok() does. But
        # doing similar, instead of a conditional on the ok() result, allows us to leverage
        # the error message building that raise_for_status() already does, before re-raising
        # the appropriate HTTPError.
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"{e} -- {resp.text}")
        if should_raise_for_status:
            raise
        else:
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
