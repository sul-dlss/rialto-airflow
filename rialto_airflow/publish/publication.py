import logging

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Publication,
    Author,
    Funder,
)
from rialto_airflow.schema.reports import RIALTO_REPORTS_DB_NAME, Publications
from rialto_airflow.utils import get_types


# NOTE: We used to write out CSV files to google drive as well.
# This was removed in https://github.com/sul-dlss/rialto-airflow/pull/528 in case
# we need it again in the future.  This PR also removed the related CSV writing tests in test_publication.py


def export_publications(snapshot) -> bool:
    """
    Export publications information to the reports publications table
    """

    logging.info("started writing publications table")

    with get_session(snapshot.database_name).begin() as select_session:
        # This query joins the publication and funder tables
        # Since we want one row per publication, and a publication can
        # have multiple funders, the booleans associated with whether they
        # are federal, are grouped together in a list using the jsonb_agg_strict
        # function (the strict version drops null values). In order to use these
        # aggregate functions we need to group by the Publication.id.

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
