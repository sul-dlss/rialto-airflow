import json
import logging
import os
import re
import time
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
                .execution_options(yield_per=25)
            )

            for rows in select_session.execute(stmt).partitions():
                # since the query uses yield_per=25 we will be looking up 25 DOIs at a time
                dois = [normalize_doi(row.doi) for row in rows]

                # The public API has a rate limit https://api.crossref.org/swagger-ui/
                # This should hopefully let us go quickly enough and stay within the limit
                # We could consider implementing support for the x-rate-limit-limit header.
                time.sleep(1.0)

                logging.info(f"looking up DOIs {dois}")
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


def get_dois(dois: list[str]) -> list[dict]:
    prefixed_dois = []
    for doi in dois:
        doi = doi.replace(",", "")  # commas are used to join DOIs in the filter

        # DOIs need to have a prefix in the API works filter call, but we don't store them that way
        if not doi.startswith("doi:"):
            doi = f"doi:{doi}"

        # According to Crossref API error messages the DOI must be of the form:
        # doi:10.prefix/suffix where prefix is 4 or more digits and suffix is a string
        if m := re.match(r"^doi:10\.(.+)/.+$", doi):
            if len(m.group(1)) >= 4:
                prefixed_dois.append(doi)
            else:
                logging.warning(f"Ignoring {doi} with invalid prefix code {m.group(1)}")
        else:
            logging.warning(f"Ignoring invalid DOI format {doi}")

    if len(prefixed_dois) == 0:
        logging.warning(f"No valid DOIs to look up in {dois}")
        return []

    params: Dict[str, str | int | None] = {
        "filter": ",".join(prefixed_dois),
        "rows": len(prefixed_dois),
        "mailto": RIALTO_EMAIL,
    }

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
        return results["message"]["items"]
    else:
        return []
