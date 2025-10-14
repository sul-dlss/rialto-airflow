import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Author,
    Funder,
    Publication,
)
from rialto_airflow.schema.reports import (
    RIALTO_REPORTS_DB_NAME,
    Publications,
    PublicationsBySchool,
    PublicationsByDepartment,
    PublicationsByAuthor
)
from rialto_airflow.utils import piped

# NOTE: We used to write out CSV files to google drive as well.
# This was removed in https://github.com/sul-dlss/rialto-airflow/pull/528 in case
# we need it again in the future.  This PR also removed the related CSV writing tests in test_publication.py


def export_publications(snapshot) -> int:
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
                Publication.types,
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
            conn = insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            conn.execute(f"TRUNCATE {Publications.__tablename__}")

            for count, row in enumerate(select_session.execute(stmt), start=1):
                row_values = {
                    "doi": row.doi,
                    "pub_year": row.pub_year,
                    "apc": row.apc,
                    "open_access": row.open_access,
                    "types": piped(row.types),
                    "federally_funded": any(row.federal),
                    "academic_council_authored": any(row.academic_council),
                    "faculty_authored": "faculty" in row.primary_role,
                }

                insert_session.execute(
                    insert(Publications).values(**row_values).on_conflict_do_nothing()
                )

        logging.info("finished writing publications table")

    return count


def export_publications_by_school(snapshot) -> int:
    """
    Export publications information to the publications_by_school table.
    """
    logging.info("started writing publications_by_school table")

    with get_session(snapshot.database_name).begin() as select_session:
        stmt = (
            select(  # type: ignore
                Publication.apc,  # type: ignore
                Publication.doi,  # type: ignore
                Publication.open_access,  # type: ignore
                Author.primary_school,
                Publication.pub_year,  # type: ignore
                Publication.types,
                # for academic_council
                func.jsonb_agg_strict(Author.academic_council).label(
                    "academic_council"
                ),
                # for federally_funded
                func.jsonb_agg_strict(Funder.federal).label("federal"),
                # for faculty_authored
                func.jsonb_agg_strict(Author.primary_role).label("roles"),
            )
            .join(Author, Publication.authors)  # type: ignore
            .join(Funder, Publication.funders, isouter=True)  # type: ignore
            .group_by(Author.primary_school, Publication.id)
            .execution_options(yield_per=100)
        )

        with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
            conn = insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            conn.execute(f"TRUNCATE {PublicationsBySchool.__tablename__}")

            for count, row in enumerate(select_session.execute(stmt), start=1):
                row_values = {
                    "academic_council_authored": any(row.academic_council),
                    "apc": row.apc,
                    "doi": row.doi,
                    "faculty_authored": "faculty" in row.roles,
                    "federally_funded": any(row.federal),
                    "open_access": row.open_access,
                    "primary_school": row.primary_school,
                    "pub_year": row.pub_year,
                    "types": piped(row.types),
                }

                insert_session.execute(
                    insert(PublicationsBySchool)
                    .values(**row_values)
                    .on_conflict_do_nothing()
                )

        logging.info("finished writing publications_by_school table")

    return count


def export_publications_by_department(snapshot) -> int:
    """
    Export publications information to the publications_by_department table.
    """
    logging.info("started writing publications_by_department table")

    with get_session(snapshot.database_name).begin() as select_session:
        stmt = (
            select(  # type: ignore
                Publication.apc,  # type: ignore
                Publication.doi,  # type: ignore
                Publication.open_access,  # type: ignore
                Author.primary_school,
                Author.primary_dept,
                Publication.pub_year,  # type: ignore
                Publication.types,  # type: ignore
                # for academic_council
                func.jsonb_agg_strict(Author.academic_council).label(
                    "academic_council"
                ),
                # for federally_funded
                func.jsonb_agg_strict(Funder.federal).label("federal"),
                # for faculty_authored
                func.jsonb_agg_strict(Author.primary_role).label("roles"),
            )
            .join(Author, Publication.authors)  # type: ignore
            .join(Funder, Publication.funders, isouter=True)  # type: ignore
            .group_by(Author.primary_school, Author.primary_dept, Publication.id)
            .execution_options(yield_per=100)
        )

        with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
            conn = insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            conn.execute(f"TRUNCATE {PublicationsByDepartment.__tablename__}")

            for count, row in enumerate(select_session.execute(stmt), start=1):
                row_values = {
                    "academic_council_authored": any(row.academic_council),
                    "apc": row.apc,
                    "doi": row.doi,
                    "faculty_authored": "faculty" in row.roles,
                    "federally_funded": any(row.federal),
                    "open_access": row.open_access,
                    "primary_school": row.primary_school,
                    "primary_department": row.primary_dept,
                    "pub_year": row.pub_year,
                    "types": piped(row.types),
                }

                insert_session.execute(
                    insert(PublicationsByDepartment)
                    .values(**row_values)
                    .on_conflict_do_nothing()
                )

        logging.info("finished writing publications_by_department table")

    return count


def export_publications_by_author(snapshot) -> int:
    """
    Export publication and author information to the publications_by_author table.
    """
    logging.info("started writing publications_by_author table")

    with get_session(snapshot.database_name).begin() as select_session:
        stmt = (
            select(  # type: ignore
                Publication.apc,  # type: ignore
                Publication.doi,  # type: ignore
                Publication.open_access,  # type: ignore
                Author.primary_school,
                Author.primary_dept,
                Author.primary_role,
                Author.sunet,
                Publication.pub_year,  # type: ignore
                Publication.types,  # type: ignore
                Publication.open_access,
                # for academic_council
                func.jsonb_agg_strict(Author.academic_council).label(
                    "academic_council"
                ),
                # for federally_funded
                func.jsonb_agg_strict(Funder.federal).label("federal"),
            )
            .join(Author, Publication.authors)  # type: ignore
            .join(Funder, Publication.funders, isouter=True)  # type: ignore
            .execution_options(yield_per=100)
        )

        with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
            conn = insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            conn.execute(f"TRUNCATE {PublicationsByAuthor.__tablename__}")

            for count, row in enumerate(select_session.execute(stmt), start=1):
                row_values = {
                    "academic_council": row.academic_council,
                    "apc": row.apc,
                    "doi": row.doi,
                    "federally_funded": any(row.federal),
                    "open_access": row.open_access,
                    "primary_school": row.primary_school,
                    "primary_department": row.primary_dept,
                    "primary_role": row.primary_role,
                    "sunet": row.sunet,
                    "pub_year": row.pub_year,
                    "types": piped(row.types),
                }

                insert_session.execute(
                    insert(PublicationsByAuthor)
                    .values(**row_values)
                    .on_conflict_do_nothing()
                )

        logging.info("finished writing publications_by_author table")

    return count
