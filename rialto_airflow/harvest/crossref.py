import json
import logging
import os
import re
import time
from collections.abc import Iterable
from itertools import batched
from pathlib import Path
from typing import Dict

import requests
from sqlalchemy import select, update

from rialto_airflow.database import Publication, get_session
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi

RIALTO_EMAIL = os.environ.get(
    "AIRFLOW_VAR_OPENALEX_EMAIL"
)  # use the same email address


def fill_in(snapshot: Snapshot) -> Path:
    """Harvest Crossref data for DOIs from other publication sources."""
    jsonl_file = snapshot.path / "crossref.jsonl"
    count = 0
    with jsonl_file.open("a") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            stmt = (
                select(Publication.doi)  # type: ignore
                .where(Publication.doi.is_not(None))  # type: ignore
                .where(Publication.crossref_json.is_(None))
                .execution_options(yield_per=1000)
            )

            for rows in select_session.execute(stmt).partitions():
                dois = [normalize_doi(row.doi) for row in rows]
                for crossref_pub in get_dois(dois):
                    doi = normalize_doi(crossref_pub.get("DOI"))
                    if doi is None:
                        logging.warning("unable to determine what DOI to update")
                        continue

                    with get_session(snapshot.database_name).begin() as update_session:
                        update_stmt = (
                            update(Publication)  # type: ignore
                            .where(Publication.doi == doi)
                            .values(crossref_json=crossref_pub)
                        )
                        update_session.execute(update_stmt)

                    count += 1
                    jsonl_output.write(json.dumps(crossref_pub) + "\n")

    logging.info(f"filled in {count} publications")

    return jsonl_file


def get_dois(dois: Iterable[str]) -> Iterable[dict]:
    # the API only allows looking up 49 DOIs at a time in a batch
    for doi_batch in batched(dois, 40):
        prefixed_dois = []
        for doi in doi_batch:
            doi = doi.replace(",", "")  # commas are used to join DOIs in the filter

            # DOIs need to have a prefix in the API works filter call, but we don't store them that way
            if not doi.startswith("doi:"):
                doi = f"doi:{doi}"

            # According to Crossref API error messages the DOI must be of the form:
            # doi:10.prefix/suffix where prefix is 4 or more digits and suffix is a string
            if m := re.match(r"^doi:10\.(\d+)/.+$", doi):
                if len(m.group(1)) >= 4:
                    prefixed_dois.append(doi)
                else:
                    logging.warning(
                        f"Ignoring {doi} with invalid prefix code {m.group(1)}"
                    )
            else:
                logging.warning(f"Ignoring invalid DOI format {doi}")

        if len(prefixed_dois) == 0:
            logging.warning(f"No valid DOIs to look up in {dois}")
            return

        logging.info(f"Looking up DOIS {prefixed_dois}")

        params: Dict[str, str | int | None] = {
            "filter": ",".join(prefixed_dois),
            "rows": len(prefixed_dois),
            "mailto": RIALTO_EMAIL,
        }

        # prevent us getting ban from public use
        time.sleep(1)

        resp = requests.get(  # type: ignore
            "https://api.crossref.org/works/",
            params=params,
            headers={
                "User-Agent": "Stanford RIALTO: https://github.com/sul-dlss/rialto-airflow"
            },
        )

        # be noisy if we don't get a 200 OK
        resp.raise_for_status()

        results = resp.json()
        if "message" in results and "items" in results.get("message", {}):
            yield from results["message"]["items"]
        else:
            logging.warn(f"Unexpected JSON response {results}")
