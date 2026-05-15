import logging
import os

import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import select, update
from urllib3.util import Retry

from rialto_airflow.database import get_session
from rialto_airflow.schema.rialto import Publication, RIALTO_DB_NAME
from rialto_airflow.utils import normalize_doi

ALTMETRIC_KEY = os.environ.get("AIRFLOW_VAR_ALTMETRIC_KEY")

# catch rate limiting HTTP 429 responses and back off
http = requests.Session()
http.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            status=10,
            status_forcelist=[429],
            backoff_factor=0.1,
            backoff_jitter=2,
            allowed_methods=["GET", "POST"],
        )
    ),
)


def fill_in() -> None:
    """
    Fetch Altmetric metadata for every DOI in the database.
    """
    count = 0
    with get_session(RIALTO_DB_NAME).begin() as select_session:
        stmt = (
            select(Publication.doi)
            .where(Publication.doi.is_not(None))
            .where(Publication.altmetric_json.is_(None))
        )

        for row in select_session.execute(stmt):
            altmetric_data = get_by_doi(row.doi)
            if altmetric_data is None:
                continue

            with get_session(RIALTO_DB_NAME).begin() as update_session:
                update_stmt = (
                    update(Publication)
                    .where(Publication.doi == row.doi)
                    .values(altmetric_json=altmetric_data)
                )
                update_session.execute(update_stmt)

            count += 1

    logging.info(f"filled in {count} publications")


def get_by_doi(doi: str) -> dict | None:
    """
    Lookup a DOI at Altmetric and return the metadata or None if Altmetric
    doesn't know anything about it.
    """
    doi_normal = normalize_doi(doi)
    if doi_normal is None:
        return None

    url = f"https://api.altmetric.com/v1/fetch/doi/{doi_normal}"
    resp = http.get(url, params={"key": ALTMETRIC_KEY})

    if resp.status_code == 404:
        return None

    resp.raise_for_status()

    return resp.json()
