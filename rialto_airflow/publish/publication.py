import logging
from csv import DictWriter
from pathlib import Path

from sqlalchemy import select, func

from rialto_airflow.database import get_session, Publication, Author, Funder
from rialto_airflow.utils import get_types


def get_csv_path(snapshot, filename) -> Path:
    """
    Get the base path for a CSV file in the shared google drive
    """
    csv_path = snapshot.path / "publication-dashboard" / filename
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    return csv_path


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

    csv_path = get_csv_path(snapshot, "publications.csv")

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
        "apc",
        "doi",
        "faculty_authored",
        "federally_funded",
        "open_access",
        "primary_school",
        "pub_year",
        "types",
    ]

    csv_path = get_csv_path(snapshot, "contributions-by-school.csv")

    logging.info(f"starting to write contributions by school {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Publication.apc,  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Author.primary_school,
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
                .group_by(Author.primary_school, Publication.id)
                .execution_options(yield_per=100)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council_authored": any(row.academic_council),
                        "apc": row.apc,
                        "doi": row.doi,
                        "faculty_authored": "faculty" in row.roles,
                        "federally_funded": any(row.federal),
                        "open_access": row.open_access,
                        "primary_school": row.primary_school,
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
        "apc",
        "doi",
        "faculty_authored",
        "federally_funded",
        "open_access",
        "primary_school",
        "primary_department",
        "pub_year",
        "types",
    ]

    csv_path = get_csv_path(snapshot, "contributions-by-school-department.csv")

    logging.info(f"starting to write contributions by school/department {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Publication.apc,  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Author.primary_school,
                    Author.primary_dept,
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
                .group_by(Author.primary_school, Author.primary_dept, Publication.id)
                .execution_options(yield_per=100)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council_authored": any(row.academic_council),
                        "apc": row.apc,
                        "doi": row.doi,
                        "faculty_authored": "faculty" in row.roles,
                        "federally_funded": any(row.federal),
                        "open_access": row.open_access,
                        "primary_school": row.primary_school,
                        "primary_department": row.primary_dept,
                        "pub_year": row.pub_year,
                        "types": "|".join(get_types(row)),
                    }
                )

        logging.info(f"finished writing contributions by department {csv_path}")

    return csv_path
