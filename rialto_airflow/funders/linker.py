import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm.session import Session


from rialto_airflow.database import get_session, Publication, Funder
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
                logging.warning("got null funders property")
                continue

            for funder in pub.dim_json.get("funders", []):
                if funder_model := _find_or_create_funder(session, funder):
                    pub.funders.append(funder_model)

    return count


def _find_or_create_funder(session: Session, funder: dict) -> Optional[Funder]:
    grid_id = funder.get("id")
    name = funder.get("name")

    if grid_id is None or name is None:
        logging.info(f"missing GRID ID in {funder}")
        return None

    funder_obj = session.query(Funder).where(Funder.grid_id == grid_id).first()  # type: ignore
    if funder_obj:
        return funder_obj

    return Funder(
        name=name,
        grid_id=grid_id,
        federal=(is_federal_grid_id(grid_id) or is_federal(name)),
    )
