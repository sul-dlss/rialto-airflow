import logging
import os
from typing import Optional

import requests
from pyalex import Funders, config
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Funder,
    Publication,
    pub_funder_association,
)
from rialto_airflow.funders.dataset import is_federal, is_federal_grid_id
from rialto_airflow.funders.ror_grid_dataset import convert_ror_to_grid

config.email = os.environ.get("AIRFLOW_VAR_OPENALEX_EMAIL")
config.max_retries = 5
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]
config.api_key = os.environ.get("AIRFLOW_VAR_OPENALEX_API_KEY")


def link_publications(snapshot) -> int:
    dim_count = link_dim_publications(snapshot)
    openalex_count = link_openalex_publications(snapshot)

    return dim_count + openalex_count


def link_dim_publications(snapshot) -> int:
    """
    Get funder info from Dimensions and link them to publications
    """
    count = 0
    with get_session(snapshot.database_name).begin() as session:
        stmt = (
            select(Publication)
            .where(Publication.dim_json.is_not(None))  # type: ignore
            .execution_options(yield_per=100)
        )

        for row in session.execute(stmt):
            count += 1
            if count % 50000 == 0:
                logging.debug(f"processing publication number {count} from Dimensions")

            pub = row[0]

            funders = pub.dim_json.get("funders", [])
            if funders is None:
                continue

            with get_session(snapshot.database_name).begin() as update_session:
                for funder in pub.dim_json.get("funders", []):
                    if funder_id := _find_or_create_dim_funder(update_session, funder):
                        update_session.execute(
                            insert(pub_funder_association)
                            .values(publication_id=pub.id, funder_id=funder_id)
                            .on_conflict_do_nothing()
                        )
    logging.debug(f"processed {count} publications from Dimensions")
    return count


def link_openalex_publications(snapshot) -> int:
    """
    Get funder info from OpenAlex and link to publications
    """
    count = 0
    with get_session(snapshot.database_name).begin() as session:
        stmt = (
            select(Publication)
            .where(Publication.openalex_json.is_not(None))  # type: ignore
            .execution_options(yield_per=100)
        )

    for row in session.execute(stmt):
        count += 1
        if count % 50000 == 0:
            logging.debug(f"processed {count} publications from OpenAlex")

        pub = row[0]

        funders = pub.openalex_json.get("grants", [])
        if not funders:
            continue

        with get_session(snapshot.database_name).begin() as update_session:
            for funder in funders:
                openalex_funder_id = funder["funder"]
                if funder_id := _find_or_create_openalex_funder(
                    update_session, openalex_funder_id
                ):
                    update_session.execute(
                        insert(pub_funder_association)
                        .values(publication_id=pub.id, funder_id=funder_id)
                        .on_conflict_do_nothing()
                    )
    logging.debug(f"processed {count} publications from OpenAlex")
    return count


def _find_or_create_dim_funder(session: Session, funder: dict) -> Optional[int]:
    grid_id = funder.get("id")
    name = funder.get("name")

    if grid_id is None or name is None:
        logging.warning(f"missing GRID ID in {funder}")
        return None

    federal = is_federal_grid_id(grid_id) or is_federal(name)

    funder_id = session.execute(
        insert(Funder)
        .values(name=name, grid_id=grid_id, federal=federal)
        .on_conflict_do_update(
            constraint="funder_grid_id_key", set_=dict(name=name, federal=federal)
        )
        .returning(Funder.id)
    ).scalar_one()

    return funder_id


def _find_or_create_openalex_funder(
    session: Session, openalex_id: str
) -> Optional[int]:
    # if the funder is in the database aleady return it
    funder = session.execute(
        select(Funder).where(Funder.openalex_id == openalex_id)
    ).scalar_one_or_none()
    if funder:
        return funder.id

    # otherwise look it up with the api
    funder = _lookup_openalex_funder(openalex_id)
    if not funder:
        logging.warning(f"No funder found in OpenAlex for {openalex_id}")
        return None

    funder_id = session.execute(
        insert(Funder)
        .values(**funder)
        .on_conflict_do_update(
            constraint="funder_grid_id_key",
            set_={k: v for k, v in funder.items() if v is not None},
        )
        .returning(Funder.id)
    ).scalar_one()

    return funder_id


def _lookup_openalex_funder(openalex_id: str) -> Optional[dict]:
    """
    Look up funder in OpenAlex and return it after trying to map it to GRID ID
    and looking up whether it appears to be a federal funder.
    """
    try:
        funder = Funders()[openalex_id]
        logging.debug(f"found funder data in openalex for {openalex_id}")
    except requests.exceptions.HTTPError:
        logging.warning(f"OpenAlex API returned error for {openalex_id}")
        return None

    # a ror_id is required
    ror_id = funder.get("ids", {}).get("ror")
    if ror_id is None:
        logging.warning(f"no ROR ID for {openalex_id}")
        return None

    # a grid_id is required
    grid_id = convert_ror_to_grid(ror_id)
    if grid_id is None:
        logging.warning(f"no GRID ID could be determined for {openalex_id}")
        return None

    name = funder.get("display_name")
    federal = is_federal_grid_id(grid_id) or is_federal(name)

    return {
        "openalex_id": openalex_id,
        "grid_id": grid_id,
        "ror_id": ror_id,
        "name": name,
        "federal": federal,
    }
