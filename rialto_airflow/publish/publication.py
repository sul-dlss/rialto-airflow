import logging

from sqlalchemy import select, func, Boolean, Column, Integer, String
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base  # type: ignore
from sqlalchemy.types import DateTime

from rialto_airflow.database import (
    RIALTO_REPORTS_DB_NAME,
    create_schema,
    get_session,
    utcnow,
    Publication,
    Author,
    Funder,
)
from rialto_airflow.utils import get_types


ReportsSchemaBase = declarative_base()

# NOTE: We used to write out CSV files to google drive as well.
# This was removed in https://github.com/sul-dlss/rialto-airflow/pull/528 in case
# we need it again in the future.  This PR also removed the related CSV writing tests in test_publication.py


class Publications(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, unique=True)
    pub_year = Column(Integer)
    apc = Column(Integer)
    open_access = Column(String)
    types = Column(String)
    federally_funded = Column(Boolean)
    academic_council_authored = Column(Boolean)
    faculty_authored = Column(Boolean)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())


def init_reports_data_schema() -> None:
    create_schema(RIALTO_REPORTS_DB_NAME, ReportsSchemaBase)


def export_publications(snapshot) -> bool:
    """
    Export publications information to the reports publications table
    """

    logging.info("started writing publications table")

    with get_session(snapshot.database_name).begin() as select_session:
        # This query joins the publication and funder tables
        # Since we want one row per publication, and a publication can
        # have multiple funders, the funder names, and the booleans
        # associated with whether they are federal, are grouped together in
        # a list using the jsonb_agg_strict function (the strict version
        # drops null values). In order to use these aggregate functions we
        # need to group by the Publication.id.

        stmt = (
            select(  # type: ignore
                Publication.doi,  # type: ignore
                Publication.pub_year,  # type: ignore
                Publication.apc,  # type: ignore
                Publication.open_access,
                Publication.dim_json["type"].label("dim_type"),
                Publication.openalex_json["type"].label("openalex_type"),
                Publication.wos_json["static_data"]["fullrecord_metadata"][
                    "normalized_doctypes"
                ]["doctype"].label("wos_type"),
                func.jsonb_agg_strict(Author.academic_council).label(
                    "academic_council"
                ),
                func.jsonb_agg_strict(Author.primary_role).label("primary_role"),
                func.jsonb_agg_strict(Funder.federal).label("federal"),
            )
            .join(Author, Publication.authors)  # type: ignore
            .join(Funder, Publication.funders, isouter=True)  # type: ignore
            .group_by(Publication.id)
            .execution_options(yield_per=10_000)
        )

        with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
            insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            insert_session.connection().execute(
                f"TRUNCATE {Publications.__tablename__}"
            )

            for row in select_session.execute(stmt):
                row_values = {
                    "doi": row.doi,
                    "pub_year": row.pub_year,
                    "apc": row.apc,
                    "open_access": row.open_access,
                    "types": "|".join(get_types(row)) or None,
                    "federally_funded": any(row.federal),
                    "academic_council_authored": any(row.academic_council),
                    "faculty_authored": "faculty" in row.primary_role,
                }

                insert_session.execute(
                    insert(Publications).values(**row_values).on_conflict_do_nothing()
                )

        logging.info("finished writing publications table")

    return True
