import logging
from csv import DictWriter
from pathlib import Path

from sqlalchemy import select, func

from rialto_airflow.database import get_session, Publication, Author, Funder


def write_publications(snapshot) -> Path:
    """
    Write a CSV of publications including their funding information.
    """
    col_names = [
        "doi",
        "pub_year",
        "apc",
        "open_access",
        "types",
        "funders",
        "federally_funded",
        "academic_council_authored",
        "faculty_authored",
    ]

    csv_path = snapshot.path / "publications.csv"
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
                    func.jsonb_agg_strict(Author.academic_council).label(
                        "academic_council"
                    ),
                    func.jsonb_agg_strict(Author.primary_role).label("primary_role"),
                    func.jsonb_agg_strict(Funder.name).label("funders"),
                    func.jsonb_agg_strict(Funder.federal).label("federal"),
                )
                .join(Author, Publication.authors)  # type: ignore
                .join(Funder, Publication.funders, isouter=True)  # type: ignore
                .where(Publication.pub_year >= 2018)
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
                        "types": "|".join(sorted(_get_types(row))) or None,
                        "funders": "|".join(sorted(set(row.funders))) or None,
                        "federally_funded": any(row.federal),
                        "academic_council_authored": any(row.academic_council),
                        "faculty_authored": "faculty" in row.primary_role,
                    }
                )

        logging.info(f"finished writing publications {csv_path}")

    return csv_path


def write_contributions(snapshot) -> Path:
    """
    Write a CSV of contributions where each row represents a publication from a particular Author.
    """

    col_names = [
        "sunet",
        "role",
        "academic_council",
        "primary_school",
        "primary_department",
        "doi",
        "pub_year",
        "apc",
        "open_access",
        "types",
        "federally_funded",
    ]

    csv_path = snapshot.path / "contributions.csv"

    logging.info(f"starting to write contributions {csv_path}")

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            # This query joins the publication, contribution and funder tables.
            # We want one row per "contribution" or unique Stanford author
            # per-publication, however a publication can
            # have multiple funders. In order to prevent there being one row per
            # funder-publication-author combinateion, the funder names, and the booleans
            # associated with whether they are federal, are grouped together in
            # a list using the jsonb_agg_strict function (the strict version
            # drops null values). Using this aggregate function then requires
            # that we group by the publication.id and author.id.

            stmt = (
                select(  # type: ignore
                    Author.sunet,  # type: ignore
                    Author.primary_role,
                    Author.academic_council,  # type: ignore
                    Author.primary_school,
                    Author.primary_dept,
                    Publication.doi,  # type: ignore
                    Publication.pub_year,  # type: ignore
                    Publication.apc,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Publication.dim_json["type"].label("dim_type"),
                    Publication.openalex_json["type"].label("openalex_type"),
                    func.jsonb_agg_strict(Funder.federal).label("federal"),
                )
                .join(Author, Publication.authors)  # type: ignore
                .join(Funder, Publication.funders, isouter=True)  # type: ignore
                .where(Publication.pub_year >= 2018)
                .group_by(Author.id, Publication.id)
                .execution_options(yield_per=100)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "sunet": row.sunet,
                        "role": row.primary_role,
                        "academic_council": row.academic_council,
                        "primary_school": row.primary_school,
                        "primary_department": row.primary_dept,
                        "doi": row.doi,
                        "pub_year": row.pub_year,
                        "apc": row.apc,
                        "open_access": row.open_access,
                        "types": "|".join(sorted(_get_types(row))),
                        "federally_funded": any(row.federal),
                    }
                )

        logging.info(f"finished writing contributions {csv_path}")

    return csv_path


def _get_types(row):
    types = set()
    if row.dim_type:
        types.add(row.dim_type)
    if row.openalex_type:
        types.add(row.openalex_type)

    return types
