import logging
from csv import DictWriter
from pathlib import Path

from sqlalchemy import select, func

from rialto_airflow.database import get_session, Publication, Author, Funder
from rialto_airflow.utils import get_types, get_csv_path, normalize_pmid
import rialto_airflow.publish.publication_utils as pub_utils


def google_drive_folder() -> str:
    return "publication-dashboard"


def write_publications(snapshot) -> Path:
    """
    Write a CSV of publications
    """
    col_names = [
        "doi",
        "pub_year",
        "apc",
        "open_access",
        "types",
        "federally_funded",
        "academic_council_authored",
        "faculty_authored",
    ]

    csv_path = get_csv_path(snapshot, google_drive_folder(), "publications.csv")

    logging.info(f"started writing publications {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
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
                .group_by(Publication.id)
                .execution_options(yield_per=100)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "doi": row.doi,
                        "pub_year": row.pub_year,
                        "apc": row.apc,
                        "open_access": row.open_access,
                        "types": "|".join(get_types(row)) or None,
                        "federally_funded": any(row.federal),
                        "academic_council_authored": any(row.academic_council),
                        "faculty_authored": "faculty" in row.primary_role,
                    }
                )

        logging.info(f"finished writing publications {csv_path}")

    return csv_path


def write_contributions_by_school(snapshot) -> Path:
    """
    Write a CSV of contributions where each row represents a unique publication per school.
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
        "role",
        "sunet",
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
                    Author.primary_dept,  # type: ignore
                    Author.primary_role,  # type: ignore
                    Author.sunet,  # type: ignore
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
                .group_by(
                    Author.primary_school,
                    Author.sunet,
                    Publication.doi,
                    Publication.id,
                    Author.id,
                )
                .execution_options(yield_per=100)
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
                        "role": row.primary_role,
                        "sunet": row.sunet,
                        "pub_year": row.pub_year,
                        "types": "|".join(get_types(row)),
                    }
                )

        logging.info(f"finished writing contributions by school {csv_path}")

    return csv_path


def write_contributions_by_department(snapshot) -> Path:
    """
    Write a CSV of contributions where each row represents a unique publication
    per school and department.
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
        "role",
        "sunet",
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
                    Author.primary_role,  # type: ignore
                    Author.sunet,  # type: ignore
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
                .group_by(
                    Author.primary_school,
                    Author.primary_dept,
                    Author.sunet,
                    Publication.doi,
                    Publication.id,
                    Author.id,
                )
                .execution_options(yield_per=100)
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
                        "role": row.primary_role,
                        "sunet": row.sunet,
                        "pub_year": row.pub_year,
                        "types": "|".join(get_types(row)),
                    }
                )

        logging.info(f"finished writing contributions by school/department {csv_path}")

    return csv_path
