import logging
import os
from typing import Optional

from pyalex import Funders, config
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert


from rialto_airflow.database import (
    get_session,
    Publication,
    Funder,
    pub_funder_association,
)
from rialto_airflow.funders.dataset import is_federal, is_federal_grid_id
from rialto_airflow.funders.ror_grid_dataset import convert_ror_to_grid


config.email = os.environ.get("AIRFLOW_VAR_OPENALEX_EMAIL")
config.max_retries = 5
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


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
            if count % 100 == 0:
                logging.info(f"processed {count} publications from Dimensions")

            pub = row[0]

            funders = pub.dim_json.get("funders", [])
            if funders is None:
                continue

            with get_session(snapshot.database_name).begin() as update_session:
                for funder in pub.dim_json.get("funders", []):
                    if funder_id := _find_or_create_funder(snapshot, funder):
                        update_session.execute(
                            insert(pub_funder_association)
                            .values(publication_id=pub.id, funder_id=funder_id)
                            .on_conflict_do_nothing()
                        )
    logging.info(f"processed {count} publications from Dimensions")
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
        if count % 100 == 0:
            logging.info(f"processed {count} publications from OpenAlex")

        pub = row[0]

        funders = pub.openalex_json.get("grants", [])
        if not funders:
            continue

        with get_session(snapshot.database_name).begin() as update_session:
            for funder in funders:
                if openalex_funder := _lookup_openalex_funder(funder):
                    logging.info(
                        f"found funder data in openalex for {openalex_funder['id']}"
                    )
                    if funder_id := _find_or_create_funder(snapshot, openalex_funder):
                        update_session.execute(
                            insert(pub_funder_association)
                            .values(publication_id=pub.id, funder_id=funder_id)
                            .on_conflict_do_nothing()
                        )
    logging.info(f"processed {count} publications from OpenAlex")
    return count


def _find_or_create_funder(snapshot, funder: dict) -> Optional[int]:
    # TODO: handle adding ROR ID? When we do we'll want to also handle unique
    # constraint errors on insert like we are with grid_id.
    grid_id = funder.get("id")
    name = funder.get("name")

    if grid_id is None or name is None:
        logging.info(f"missing GRID ID in {funder}")
        return None

    federal = is_federal_grid_id(grid_id) or is_federal(name)

    with get_session(snapshot.database_name).begin() as session:
        funder_id = session.execute(
            insert(Funder)
            .values(name=name, grid_id=grid_id, federal=federal)
            .on_conflict_do_update(
                constraint="funder_grid_id_key", set_=dict(name=name, federal=federal)
            )
            .returning(Funder.id)
        ).scalar_one()

    return funder_id


def _lookup_openalex_funder(funder: dict) -> Optional[dict]:
    """
    Look up funder in OpenAlex to get ROR and then map to GRID ID
    """
    openalex_funder = Funders()[funder.get("funder")]
    name = openalex_funder.get("display_name")
    ror = openalex_funder.get("ids", {}).get("ror", None)
    if ror:
        grid = convert_ror_to_grid(ror)
        if grid is None:
            logging.info(f"missing GRID ID for {funder}")
            return None
        # TODO: add ROR ID to funder
        funder_data = {"id": grid, "name": name}

        return funder_data
    else:
        logging.info(f"no ROR ID for {funder}")
        return None
