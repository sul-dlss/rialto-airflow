import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.session import Session


from rialto_airflow.database import (
    get_session,
    Publication,
    Funder,
    pub_funder_association,
)
from rialto_airflow.funders.dataset import is_federal, is_federal_grid_id


def link_publications(snapshot) -> int:
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
                logging.info(f"processed {count} publications")

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
