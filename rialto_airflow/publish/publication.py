import logging
from csv import DictWriter
from pathlib import Path

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
from rialto_airflow.utils import get_types, get_csv_path, normalize_pmid
import rialto_airflow.publish.publication_utils as pub_utils


ReportsSchemaBase = declarative_base()


class Publications(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, unique=True)
    pub_year = Column(Integer)
    apc = Column(Integer)
    open_access = Column(String)
    types = Column(String)
    funders = Column(String)
    federally_funded = Column(Boolean)
    academic_council_authored = Column(Boolean)
    faculty_authored = Column(Boolean)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())


def google_drive_folder() -> str:
    return "publication-dashboard"


def init_reports_data_schema() -> None:
    create_schema(RIALTO_REPORTS_DB_NAME, ReportsSchemaBase)


def write_publications(snapshot) -> bool:
    """
    Write publications information to the reports publications table
    """

    # NOTE: We used to write out this data to a CSV file in google drive as well.
    # This was removed in https://github.com/sul-dlss/rialto-airflow/pull/528 in case
    # we need it again in the future.
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
                func.jsonb_agg_strict(Funder.name).label("funders"),
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
                    "funders": "|".join(sorted(set(row.funders))),
                    "federally_funded": any(row.federal),
                    "academic_council_authored": any(row.academic_council),
                    "faculty_authored": "faculty" in row.primary_role,
                }

                insert_session.execute(
                    insert(Publications).values(**row_values).on_conflict_do_nothing()
                )

        logging.info("finished writing publications table")

    return True


def write_contributions_by_school(snapshot) -> Path:
    """
    Write a CSV where there is a unique row for each DOI and school.
    """

    col_names = [
        "academic_council_authored",
        "academic_council",
        "journal",
        "issue",
        "mesh",
        "pages",
        "volume",
        "apc",
        "doi",
        "pmid",
        "title",
        "url",
        "faculty_authored",
        "federally_funded",
        "open_access",
        "primary_school",
        "pub_year",
        "types",
    ]

    csv_path = get_csv_path(
        snapshot, google_drive_folder(), "contributions-by-school.csv"
    )

    logging.info(f"starting to write contributions by school {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Publication.apc,  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.title,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Publication.dim_json,
                    Publication.openalex_json,
                    Publication.sulpub_json,  # type: ignore
                    Publication.wos_json,  # type: ignore
                    Publication.pubmed_json,  # type: ignore
                    Author.primary_school,  # type: ignore
                    Publication.pub_year,  # type: ignore
                    # for academic_council
                    func.jsonb_agg_strict(Author.academic_council).label(
                        "academic_council"
                    ),
                    # for publication types
                    Publication.dim_json["type"].label("dim_type"),
                    Publication.openalex_json["type"].label("openalex_type"),
                    Publication.wos_json["static_data"]["fullrecord_metadata"][
                        "normalized_doctypes"
                    ]["doctype"].label("wos_type"),
                    # for federally_funded
                    func.jsonb_agg_strict(Funder.federal).label("federal"),
                    # for faculty_authored
                    func.jsonb_agg_strict(Author.primary_role).label("roles"),
                )
                .join(Author, Publication.authors)  # type: ignore
                .join(Funder, Publication.funders, isouter=True)  # type: ignore
                .group_by(
                    Author.primary_school,
                    Publication.id,  # requirement is to group by DOI, but grouping by pub ID is the same, since DOI is unique
                )
                .execution_options(yield_per=10_000)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council_authored": any(row.academic_council),
                        "academic_council": row.academic_council,
                        "journal": pub_utils._journal(row),
                        "issue": pub_utils._issue(row),
                        "mesh": pub_utils._mesh(row),
                        "pages": pub_utils._pages(row),
                        "volume": pub_utils._volume(row),
                        "apc": row.apc,
                        "doi": row.doi,
                        "pmid": normalize_pmid(pub_utils._pmid(row)),
                        "title": row.title,
                        "url": pub_utils._url(row),
                        "faculty_authored": "faculty" in row.roles,
                        "federally_funded": any(row.federal),
                        "open_access": row.open_access,
                        "primary_school": row.primary_school,
                        "pub_year": row.pub_year,
                        "types": "|".join(get_types(row)),
                    }
                )
                output.flush()

        logging.info(f"finished writing contributions by school {csv_path}")

    return csv_path


def write_contributions_by_department(snapshot) -> Path:
    """
    Write a CSV where there is a unique row for each DOI, school and department.
    """

    col_names = [
        "academic_council_authored",
        "academic_council",
        "journal",
        "issue",
        "mesh",
        "pages",
        "volume",
        "apc",
        "doi",
        "pmid",
        "title",
        "url",
        "faculty_authored",
        "federally_funded",
        "open_access",
        "primary_school",
        "primary_department",
        "pub_year",
        "types",
    ]

    csv_path = get_csv_path(
        snapshot, google_drive_folder(), "contributions-by-school-department.csv"
    )

    logging.info(f"starting to write contributions by school/department {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Publication.apc,  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.title,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Publication.dim_json,
                    Publication.openalex_json,
                    Publication.sulpub_json,  # type: ignore
                    Publication.wos_json,  # type: ignore
                    Publication.pubmed_json,  # type: ignore
                    Author.primary_school,  # type: ignore
                    Author.primary_dept,  # type: ignore
                    Publication.pub_year,  # type: ignore
                    # for academic_council
                    func.jsonb_agg_strict(Author.academic_council).label(
                        "academic_council"
                    ),
                    # for publication types
                    Publication.dim_json["type"].label("dim_type"),
                    Publication.openalex_json["type"].label("openalex_type"),
                    Publication.wos_json["static_data"]["fullrecord_metadata"][
                        "normalized_doctypes"
                    ]["doctype"].label("wos_type"),
                    # for federally_funded
                    func.jsonb_agg_strict(Funder.federal).label("federal"),
                    # for faculty_authored
                    func.jsonb_agg_strict(Author.primary_role).label("roles"),
                )
                .join(Author, Publication.authors)  # type: ignore
                .join(Funder, Publication.funders, isouter=True)  # type: ignore
                .group_by(
                    Author.primary_dept,
                    Author.primary_school,
                    Publication.id,  # requirement is to group by DOI, but grouping by pub ID is the same, since DOI is unique
                )
                .execution_options(yield_per=10_000)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council_authored": any(row.academic_council),
                        "academic_council": row.academic_council,
                        "journal": pub_utils._journal(row),
                        "issue": pub_utils._issue(row),
                        "mesh": pub_utils._mesh(row),
                        "pages": pub_utils._pages(row),
                        "volume": pub_utils._volume(row),
                        "apc": row.apc,
                        "doi": row.doi,
                        "pmid": normalize_pmid(pub_utils._pmid(row)),
                        "title": row.title,
                        "url": pub_utils._url(row),
                        "faculty_authored": "faculty" in row.roles,
                        "federally_funded": any(row.federal),
                        "open_access": row.open_access,
                        "primary_school": row.primary_school,
                        "primary_department": row.primary_dept,
                        "pub_year": row.pub_year,
                        "types": "|".join(get_types(row)),
                    }
                )
                output.flush()

        logging.info(f"finished writing contributions by school/department {csv_path}")

    return csv_path


def write_contributions(snapshot) -> Path:
    """
    Write a CSV where there is a unique row for each DOI and SUNET.
    """
    col_names = [
        "academic_council",
        "apc",
        "doi",
        "federally_funded",
        "funders",
        "issue",
        "journal",
        "mesh",
        "open_access",
        "pages",
        "pmid",
        "primary_department",
        "primary_school",
        "pub_year",
        "role",
        "sunet",
        "title",
        "types",
        "url",
        "volume",
    ]

    csv_path = get_csv_path(snapshot, google_drive_folder(), "contributions.csv")

    logging.info(f"started writing contributions {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.title,
                    Publication.pub_year,  # type: ignore
                    Publication.apc,  # type: ignore
                    Publication.open_access,
                    # all the JSON for extracting things like journal, issue
                    Publication.dim_json,
                    Publication.openalex_json,  # type: ignore
                    Publication.sulpub_json,  # type: ignore
                    Publication.wos_json,  # type: ignore
                    Publication.pubmed_json,  # type: ignore
                    # for publication type
                    Publication.dim_json["type"].label("dim_type"),
                    Publication.openalex_json["type"].label("openalex_type"),
                    Publication.wos_json["static_data"]["fullrecord_metadata"][
                        "normalized_doctypes"
                    ]["doctype"].label("wos_type"),
                    # for the author
                    Author.sunet,
                    Author.academic_council,
                    Author.primary_role,
                    Author.primary_school,
                    Author.primary_dept,
                    Author.academic_council,
                    # aggregate funders
                    func.jsonb_agg_strict(Funder.name).label("funders"),
                    func.jsonb_agg_strict(Funder.federal).label("federal"),
                )
                .join(Author, Publication.authors)  # type: ignore
                .join(Funder, Publication.funders, isouter=True)  # type: ignore
                .group_by(Publication.id, Author.id)
                .execution_options(yield_per=10_000)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council": row.academic_council,
                        "apc": row.apc,
                        "doi": row.doi,
                        "federally_funded": any(row.federal),
                        "funders": "|".join(sorted(set(row.funders))),
                        "issue": pub_utils._issue(row),
                        "journal": pub_utils._journal(row),
                        "mesh": pub_utils._mesh(row),
                        "open_access": row.open_access,
                        "pages": pub_utils._pages(row),
                        "pmid": normalize_pmid(pub_utils._pmid(row)),
                        "primary_department": row.primary_dept,
                        "primary_school": row.primary_school,
                        "pub_year": row.pub_year,
                        "role": row.primary_role,
                        "sunet": row.sunet,
                        "title": row.title,
                        "types": "|".join(get_types(row)) or None,
                        "url": pub_utils._url(row),
                        "volume": pub_utils._volume(row),
                    }
                )
                output.flush()

        logging.info(f"finished writing contributions {csv_path}")

    return csv_path
