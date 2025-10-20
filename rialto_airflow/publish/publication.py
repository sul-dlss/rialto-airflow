import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.distiller import FuncRule, JsonPathRule, all, first, json_path
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
    PublicationsByAuthor,
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
                Publication.academic_council_authored,
                Publication.publisher,  # type: ignore
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

    with get_session(snapshot.database_name).begin() as select_session:
        stmt = (
            select(
                Publication.apc,  # type: ignore
                Publication.doi,
                Publication.open_access,  # type: ignore
                Publication.title,  # type: ignore
                Author.primary_school,
                Author.primary_dept,
                Author.primary_role,
                Author.sunet,  # type: ignore
                Author.academic_council,  # type: ignore
                Publication.pub_year,  # type: ignore
                Publication.types,  # type: ignore
                Publication.openalex_json,
                Publication.dim_json,
                Publication.pubmed_json,
                Publication.sulpub_json,
                Publication.crossref_json,
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
                    "abstract": _abstract(row),
                    "academic_council": row.academic_council,
                    "apc": row.apc,
                    "doi": row.doi,
                    "federally_funded": any(row.federal),
                    "journal_issn": _journal_issn(row),
                    "journal_name": _journal_name(row),
                    "open_access": row.open_access,
                    "pages": _pages(row),
                    "primary_school": row.primary_school,
                    "primary_department": row.primary_dept,
                    "role": row.primary_role,
                    "sunet": row.sunet,
                    "pub_year": row.pub_year,
                    "title": row.title,
                    "types": piped(row.types),
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


def _abstract(row):
    """
    Get the abstract from openalex, dimensions, pubmed then crossref.
    """
    return first(
        row,
        rules=[
            FuncRule("openalex_json", _rebuild_abstract),
            JsonPathRule("dim_json", "abstract"),
            FuncRule("pubmed_json", _pubmed_abstract),
            JsonPathRule("crossref_json", "abstract"),
        ],
    )


def _pubmed_abstract(pubmed_json: dict) -> str:
    """
    Get the abstract from PubMed JSON.
    """
    if pubmed_json is None:
        return None

    full_abstract = []
    jsonp = json_path("MedlineCitation.Article.Abstract.AbstractText[*]")
    abstract_text = jsonp.find(pubmed_json)
    for abstract in abstract_text:
        if abstract.value is None:
            continue
        # sometimes the abstract is a string and not a dict of text segments
        if isinstance(abstract.value, str):
            full_abstract.append(abstract.value)
        else:
            full_abstract.append(abstract.value.get("#text", None))

    return " ".join(full_abstract)


def _rebuild_abstract(openalex_json: dict) -> str:
    """
    Rebuilds an abstract from a positional inverted index.
    """

    # openalex metadata isn't always defined
    if openalex_json is None:
        return None

    # guard against the abstract data being missing
    inverted_index = openalex_json.get("abstract_inverted_index")
    if inverted_index is None:
        return None

    # Create a list to hold the words in their correct positions.
    # We find the max position to make sure the list is large enough.
    max_position = 0
    for positions in inverted_index.values():
        if positions:
            max_position = max(max_position, max(positions))

    # The list is zero-indexed, so we need max_position + 1 length.
    abstract_words = [""] * (max_position + 1)

    # Place each word in its correct position.
    for word, positions in inverted_index.items():
        for position in positions:
            abstract_words[position] = word

    # Join the words to form the final abstract.
    return " ".join(abstract_words)


def _journal_issn(row) -> str:
    # get all ISSNs available and return unique values as pipe delimited string
    rules = [
        JsonPathRule("openalex_json", "primary_location.source.issn_l"),
        JsonPathRule("openalex_json", "primary_location.source.issn"),  # list
        JsonPathRule("sulpub_json", "issn"),
        JsonPathRule("dim_json", "issn"),
        JsonPathRule("crossref_json", "ISSN"),  # list
    ]

    all_issns = all(row, rules=rules)

    flat_issns = []
    for issn in all_issns:
        if isinstance(issn, list):
            flat_issns.extend(issn)
        elif issn is not None:
            flat_issns.append(issn)

    unique_issns = sorted(list(set(flat_issns)))

    return piped(unique_issns)


def _journal_name(row) -> str:
    # get journal name field from OpenAlex sources
    return first(
        row,
        rules=[
            JsonPathRule(
                "openalex_json",
                "locations[?@.source.type == 'journal'].source.display_name",
            ),
        ],
    )


def _pages(row) -> str:
    return first(
        row,
        rules=[
            FuncRule("openalex_json", _openalex_pages),
            JsonPathRule("dim_json", "pages"),
            JsonPathRule("sulpub_json", "journal.pages"),
        ],
    )


def _openalex_pages(openalex_json: dict) -> str:
    start_page = first(openalex_json, JsonPathRule("biblio.first_page"))
    end_page = first(openalex_json, JsonPathRule("biblio.last_page"))
    if start_page and end_page:
        return f"{start_page}-{end_page}"
    elif start_page:
        return start_page
    return None
