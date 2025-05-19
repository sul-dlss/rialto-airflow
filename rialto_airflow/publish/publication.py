import logging
from csv import DictWriter
from pathlib import Path
# import jsonpath_ng

from sqlalchemy import select, func

from rialto_airflow.database import get_session, Publication, Author, Funder
from rialto_airflow.utils import get_types, get_csv_path, normalize_pmid
from rialto_airflow.distiller import first, JsonPathRule, FuncRule
from rialto_airflow.harvest.pubmed import get_identifier


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
        "apc",
        "doi",
        "pmid",
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
                    Publication.open_access,  # type: ignore
                    Publication.dim_json,
                    Publication.openalex_json,
                    Publication.sulpub_json,
                    Publication.wos_json,  # type: ignore
                    Author.primary_school,  # type: ignore
                    Author.primary_department,  # type: ignore
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
                .group_by(Author.primary_school, Publication.id)
                .execution_options(yield_per=100)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council_authored": any(row.academic_council),
                        "academic_council": row.academic_council,
                        "journal": _journal(row),
                        "issue": _issue(row),
                        "mesh": _mesh(row),
                        "pages": _pages(row),
                        "apc": row.apc,
                        "doi": row.doi,
                        "pmid": normalize_pmid(_pmid(row)),
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
        "apc",
        "doi",
        "pmid",
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
                    Publication.open_access,  # type: ignore
                    Publication.dim_json,
                    Publication.openalex_json,
                    Publication.sulpub_json,
                    Publication.wos_json,  # type: ignore
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
                .group_by(Author.primary_school, Author.primary_dept, Publication.id)
                .execution_options(yield_per=100)
            )

            for row in session.execute(stmt):
                csv_output.writerow(
                    {
                        "academic_council_authored": any(row.academic_council),
                        "academic_council": row.academic_council,
                        "journal": _journal(row),
                        "issue": _issue(row),
                        "mesh": _mesh(row),
                        "pages": _pages(row),
                        "apc": row.apc,
                        "doi": row.doi,
                        "pmid": normalize_pmid(_pmid(row)),
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

        logging.info(f"finished writing contributions by department {csv_path}")

    return csv_path


def _journal(row):
    """
    Get the journal name in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "journal.title"),
            FuncRule("openalex_json", _openalex_journal),
            FuncRule("wos_json", _wos_journal),
            JsonPathRule("sulpub_json", "journal.name"),
        ],
    )


def _openalex_journal(openalex_json):
    if not openalex_json:
        return None

    try:
        primary_location = openalex_json["primary_location"]
        if primary_location["source"]["type"] == "journal":
            return primary_location["source"]["display_name"]
    except KeyError:
        logging.warning("[title] OpenAlex JSON does not contain primary location")
        return None

    return None


def _wos_journal(wos_json):
    if not wos_json:
        return None

    try:
        for title in wos_json["static_data"]["summary"]["titles"]["title"]:
            if title["type"] == "source":
                return title["content"]
    except KeyError:
        logging.warning("[title] WOS JSON does not contain journal title")
        return None

    return None


def _issue(row):
    """
    Get the journal issue in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "issue"),
            JsonPathRule("openalex_json", "biblio.issue"),
            JsonPathRule("wos_json", "static_data.summary.pub_info.issue"),
            JsonPathRule("sulpub_json", "journal.issue"),
        ],
    )


def _mesh(row):
    """
    Get the mesh in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            FuncRule("dim_json", _dim_mesh),
            FuncRule("openalex_json", _openalex_mesh),
        ],
    )


def _dim_mesh(dim_json):
    if not dim_json:
        return None

    try:
        return "|".join(list(map(lambda x: x, dim_json["mesh_terms"])))
    except KeyError:
        logging.warning("[mesh] Dimensions JSON does not contain mesh terms")
        return None


def _openalex_mesh(openalex_json):
    if not openalex_json:
        return None

    try:
        return "|".join(
            list(map(lambda x: x["descriptor_name"], openalex_json["mesh"]))
        )
    except KeyError:
        logging.warning("[mesh] OpenAlex JSON does not contain mesh terms")
        return None


def _pages(row):
    """
    Get the pages in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "pages"),
            FuncRule("openalex_json", _openalex_pages),
            FuncRule("wos_json", _wos_pages),
            JsonPathRule("sulpub_json", "journal.pages"),
        ],
    )


def _openalex_pages(openalex_json):
    if not openalex_json:
        return None

    try:
        return f"{openalex_json['biblio']['first_page']}-{openalex_json['biblio']['last_page']}"
    except KeyError:
        logging.warning("[pages] OpenAlex JSON does not contain pages")
        return None


def _wos_pages(wos_json):
    if not wos_json:
        return None

    try:
        return f"{wos_json['static_data']['summary']['pub_info']['page']['begin']}-{wos_json['static_data']['summary']['pub_info']['page']['end']}"
    except KeyError:
        logging.warning("[mesh] WOS JSON does not contain pages")
        return None


def _pmid(row):
    """
    Get the pmid in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            FuncRule("pubmed_json", _pubmed_pmid),
            JsonPathRule("dim_json", "pmid"),
            JsonPathRule("openalex_json", "ids.pmid"),
            JsonPathRule("sulpub_json", "pmid"),
            FuncRule("wos_json", _wos_pmid),
        ],
    )


def _pubmed_pmid(pubmed_json):
    if not pubmed_json:
        return None

    return get_identifier(pubmed_json, "pubmed")


def _wos_pmid(wos_json):
    if not wos_json:
        return None

    try:
        for identifier in wos_json["dynamic_data"]["cluster_related"]["identifiers"][
            "identifier"
        ]:
            if identifier["type"] == "pmid":
                return identifier["value"]
    except KeyError:
        logging.warning("[pmid] WOS JSON does not contain pmid")
        return None

    return None
