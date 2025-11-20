import logging
import zipfile
import os
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import create_engine, db_uri, get_session
from rialto_airflow.distiller import (
    abstract,
    pages,
    volume,
    issue,
    citation_count,
    author_list_names,
    first_author_name,
    last_author_name,
    author_list_orcids,
    first_author_orcid,
    last_author_orcid,
)
from rialto_airflow.schema.harvest import (
    Author,
    Funder,
    Publication,
)
from rialto_airflow.schema.reports import (
    RIALTO_REPORTS_DB_NAME,
    Publications,
    PublicationsByAuthor,
    PublicationsByDepartment,
    PublicationsBySchool,
)
from rialto_airflow.utils import downloads_dir, piped

# NOTE: We used to write out CSV files to google drive as well.
# This was removed in https://github.com/sul-dlss/rialto-airflow/pull/528 in case
# we need it again in the future.  This PR also removed the related CSV writing tests in test_publication.py


def export_publications(snapshot) -> int:
    """
    Export publications information to the reports publications table
    """

    logging.info("started writing publications table")
    count = 0

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
                Publication.academic_council_authored,
                Publication.publisher,  # type: ignore
                Publication.journal_name,  # type: ignore
                Publication.faculty_authored,  # type: ignore
                func.jsonb_agg_strict(Funder.federal).label("federal"),
            )
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
                    "publisher": row.publisher,
                    "journal_name": row.journal_name,
                    "federally_funded": any(row.federal),
                    "academic_council_authored": row.academic_council_authored,
                    "faculty_authored": row.faculty_authored,
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
    count = 0

    with get_session(snapshot.database_name).begin() as select_session:
        stmt = (
            select(  # type: ignore
                Publication.apc,  # type: ignore
                Publication.doi,  # type: ignore
                Publication.open_access,  # type: ignore
                Author.primary_school,
                Publication.pub_year,  # type: ignore
                Publication.types,
                Publication.academic_council_authored,  # type: ignore
                Publication.faculty_authored,  # type: ignore
                # for federally_funded
                func.jsonb_agg_strict(Funder.federal).label("federal"),
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
                    "academic_council_authored": row.academic_council_authored,
                    "apc": row.apc,
                    "doi": row.doi,
                    "faculty_authored": row.faculty_authored,
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
    count = 0

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
                Publication.academic_council_authored,  # type: ignore
                Publication.faculty_authored,  # type: ignore
                # for federally_funded
                func.jsonb_agg_strict(Funder.federal).label("federal"),
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
                    "academic_council_authored": row.academic_council_authored,
                    "apc": row.apc,
                    "doi": row.doi,
                    "faculty_authored": row.faculty_authored,
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
    count = 0

    with get_session(snapshot.database_name).begin() as select_session:
        stmt = (
            select(
                Publication.apc,  # type: ignore
                Publication.doi,
                Publication.open_access,  # type: ignore
                Publication.title,  # type: ignore
                Author.orcid,
                Author.primary_school,
                Author.primary_dept,  # type: ignore
                Author.primary_role,  # type: ignore
                Author.sunet,  # type: ignore
                Author.academic_council,  # type: ignore
                Publication.pub_year,  # type: ignore
                Publication.publisher,  # type: ignore
                Publication.journal_name,  # type: ignore
                Publication.types,  # type: ignore
                Publication.openalex_json,  # type: ignore
                Publication.dim_json,
                Publication.pubmed_json,
                Publication.sulpub_json,
                Publication.wos_json,
                Publication.crossref_json,
                Publication.wos_json,
                func.jsonb_agg_strict(Funder.federal).label("federal"),
            )
            .join(Author, Publication.authors)  # type: ignore
            .join(Funder, Publication.funders, isouter=True)  # type: ignore
            .group_by(Publication.id, Author.id)
            .execution_options(yield_per=100)
        )

        with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
            conn = insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            conn.execute(f"TRUNCATE {PublicationsByAuthor.__tablename__}")

            for count, row in enumerate(select_session.execute(stmt), start=1):
                row_values = {
                    "abstract": abstract(row),
                    "author_list_names": piped(author_list_names(row)),
                    "author_list_orcids": piped(author_list_orcids(row)),
                    "academic_council": row.academic_council,
                    "apc": row.apc,
                    "citation_count": citation_count(row),
                    "doi": row.doi,
                    "federally_funded": any(row.federal),
                    "first_author_name": first_author_name(row),
                    "first_author_orcid": first_author_orcid(row),
                    "issue": issue(row),
                    "last_author_name": last_author_name(row),
                    "last_author_orcid": last_author_orcid(row),
                    "journal_name": row.journal_name,
                    "open_access": row.open_access,
                    "orcid": row.orcid,
                    "pages": pages(row),
                    "primary_school": row.primary_school,
                    "primary_department": row.primary_dept,
                    "publisher": row.publisher,
                    "role": row.primary_role,
                    "sunet": row.sunet,
                    "pub_year": row.pub_year,
                    "title": row.title,
                    "types": piped(row.types),
                    "volume": volume(row),
                }

                insert_session.execute(
                    insert(PublicationsByAuthor)
                    .values(**row_values)
                    .on_conflict_do_nothing()
                )

        logging.info(
            f"finished writing {count} rows to the publications_by_author table"
        )

    return count


def generate_download_files(data_dir) -> None:
    """
    Generate download files for publications data.
    """
    TABLES = [
        "publications",
        "publications_by_department",
        "publications_by_school",
        "publications_by_author",
    ]

    # Using raw_connection for access to psycopg2 cursor's copy_expert method
    conn = create_engine(db_uri(RIALTO_REPORTS_DB_NAME), echo=False).raw_connection()
    cursor = conn.cursor()
    for table in TABLES:
        filepath = f"{downloads_dir(data_dir)}/{table}.csv"
        copy_stmt = f"COPY {table} TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');"
        # Open the output file in write mode
        with open(f"{filepath}", "w") as f:
            # Execute the COPY command and stream the output directly to the file
            cursor.copy_expert(copy_stmt, f)
            logging.info(f"Generated download file at {filepath}")

    for table in TABLES:
        filepath = f"{downloads_dir(data_dir)}/{table}.csv"
        zip_temp_filepath = f"{downloads_dir(data_dir)}/{table}-temp.zip"

        with zipfile.ZipFile(zip_temp_filepath, "w") as zipf:
            zipf.write(filepath, arcname=os.path.basename(filepath))
        zip_filepath = f"{downloads_dir(data_dir)}/{table}.zip"
        # Move the temp zip to the final zip filepath and delete the original CSV
        os.rename(zip_temp_filepath, zip_filepath)
        os.remove(filepath)
        logging.info(f"Generated zip file at {zip_filepath}")
