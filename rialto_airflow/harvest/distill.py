import logging
from typing import Optional

from sqlalchemy import select, update

from rialto_airflow.apc import get_apc
from rialto_airflow.database import get_session
from rialto_airflow.distiller import FuncRule, JsonPathRule, all, first, json_path
from rialto_airflow.harvest.openalex import source_by_issn
from rialto_airflow.schema.harvest import Publication
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import piped


def distill(snapshot: Snapshot) -> int:
    """
    Walk through all publications in the database and set the title, pub_year,
    open_access columns using the harvested metadata.
    """
    with get_session(snapshot.database_name).begin() as select_session:
        # iterate through publictions 100 at a time
        count = 0
        stmt = select(Publication).execution_options(yield_per=100)  # type: ignore

        for row in select_session.execute(stmt):
            count += 1
            if count % 50000 == 0:
                logging.debug(f"processed {count} publications")

            pub = row[0]

            # populate new publication columns
            cols = {
                "title": _title(pub),
                "pub_year": _pub_year(pub),
                "open_access": _open_access(pub),
                "types": _types(pub),
                "publisher": _publisher(pub),
                "journal_name": _journal_name(pub),
                "academic_council_authored": _academic_council(pub),
                "faculty_authored": _faculty_authored(pub),
            }

            # pub_year and open_access in cols is needed to determine the apc
            cols["apc"] = _apc(pub, cols)

            # update the publication with the new columns
            with get_session(snapshot.database_name).begin() as update_session:
                update_stmt = (
                    update(Publication)  # type: ignore
                    .where(Publication.id == pub.id)
                    .values(cols)
                )
                update_session.execute(update_stmt)

    return count


#
# Functions for determining publication attributes.
#
# TODO: would it speed things up to define the list of rules outside of the
# function so they aren't being reinstantiated all the time?
#


def _title(pub):
    """
    Get the title from sulpub, dimensions, openalex, then wos.
    """
    return first(
        pub,
        rules=[
            JsonPathRule("sulpub_json", "title"),
            JsonPathRule("sulpub_json", "booktitle"),
            JsonPathRule("dim_json", "title"),
            JsonPathRule("openalex_json", "title"),
            FuncRule("wos_json", _wos_title),
        ],
    )


def _pub_year(pub):
    """
    Get the pub_year from dimensions, openalex, wos and then sul-pub
    """
    return first(
        pub,
        rules=[
            JsonPathRule("dim_json", "year", is_valid_year=True),
            JsonPathRule("openalex_json", "publication_year", is_valid_year=True),
            JsonPathRule(
                "wos_json", "static_data.summary.pub_info.pubyear", is_valid_year=True
            ),
            JsonPathRule("sulpub_json", "year", is_valid_year=True),
            JsonPathRule("sulpub_json", "journal.year", is_valid_year=True),
        ],
    )


def _open_access(pub):
    """
    Get the _open_access value from openalex and then dimensions.
    """
    return first(
        pub,
        rules=[
            JsonPathRule("openalex_json", "open_access.oa_status"),
            FuncRule("dim_json", _open_access_dim),
        ],
    )


def _apc(pub, context):
    """
    Get the APC cost from one place in the openalex data, an external dataset, or another place in
    OpenAlex data.
    """
    # https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/CR1MMV
    first_match_apc = first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json", "apc_paid.value_usd", only_positive_number=True
            ),
            FuncRule("dim_json", _apc_oa_dataset, context),
            JsonPathRule(
                "openalex_json", "apc_list.value_usd", only_positive_number=True
            ),
        ],
    )
    # non-OA publications should not have an APC charge recorded
    oa = (context.get("open_access") or "").lower()
    is_int = isinstance(first_match_apc, int)

    if is_int and oa == "closed":
        return 0
    elif is_int:
        return first_match_apc
    elif oa == "diamond":
        return 0
    elif oa == "gold":
        return 2450
    elif oa == "hybrid":
        return 3600
    else:
        return None


#
# Helper functions to extract from specific JSON data structures that
# aren't workable using JSON Path by itself.
#


def _wos_title(wos_json):
    jsonp = json_path("static_data.summary.titles[*].title[*]")
    for title in jsonp.find(wos_json):
        if isinstance(title.value, dict) and title.value.get("type") == "item":
            return title.value.get("content")

    return None


def _open_access_dim(dim_json: dict) -> Optional[str]:
    """
    Get the open access value, but ignore "oa_all"
    """
    jsonp = json_path("open_access[*]")
    for oa in jsonp.find(dim_json):
        if oa.value and oa.value != "oa_all":
            return oa.value

    return None


def _apc_oa_dataset(dim_json, context):
    if dim_json is None:
        return None

    pub_year = context.get("pub_year")

    # we need a pub_year to determine apc
    if pub_year is None:
        return

    # sometimes issn is None instead of a list 🤷
    if dim_json.get("issn") is None:
        return

    for issn in dim_json.get("issn", []):
        cost = get_apc(issn, pub_year)
        if cost:
            return cost

    return None


def _types(pub) -> list[str]:
    types = first(
        pub,
        rules=[
            JsonPathRule("dim_json", "type"),
            JsonPathRule("openalex_json", "type"),
            FuncRule("pubmed_json", _pubmed_type),
            JsonPathRule(
                "wos_json",
                "static_data.fullrecord_metadata.normalized_doctypes.doctype",
            ),
            JsonPathRule("crossref_json", "type"),
            JsonPathRule("sulpub_json", "type"),
        ],
    )

    if types is None:
        return []

    if isinstance(types, str):
        types = [types]

    elif not isinstance(types, list):
        raise Exception(
            f"types distill rules generated unexpected result: {type(types)}"
        )

    return sorted(set([_normalize_type(s) for s in types if s is not None]))


def _pubmed_type(pubmed_json: dict) -> list[str]:
    """
    PubMed publication types can point to a dictionary or a list of dictionaries.
    """
    types = []
    jsonp = json_path("MedlineCitation.Article.PublicationTypeList.PublicationType[*]")
    for pub_type in jsonp.find(pubmed_json):
        types.append(pub_type.value.get("#text"))

    return types


def _publisher(pub):
    """
    Get the publisher from OpenAlex json if it exists
    """
    publisher = first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json", "primary_location.source.host_organization_name"
            ),
        ],
    )

    if publisher:
        return publisher
    else:
        return _publisher_by_issn(pub)


def _publisher_by_issn(row) -> Optional[str]:
    """
    # look up publisher in OpenAlex by ISSN
    """
    issn = _journal_issn(row)
    source = source_by_issn(issn)
    return source.get("host_organization_name") if source else None


def _academic_council(pub) -> Optional[bool]:
    """
    Get the academic_council value for all authors and return True if any are academic_council
    """
    authors = pub.authors

    for author in authors:
        if author.academic_council:
            return True

    return False


def _faculty_authored(pub) -> Optional[bool]:
    return any(author.primary_role == "faculty" for author in pub.authors)


def _journal_name(pub) -> str | None:
    # try to get journal name from openalex_json before querying
    openalex_journal_name = first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json",
                "locations[?@.source.type == 'journal'].source.display_name",
            ),
        ],
    )
    if openalex_journal_name:
        # we can assume OpenAlex journal names are strings but mypy cannot
        return str(openalex_journal_name)

    # get ISSN and look up journal name in OpenAlex
    issn = _journal_issn(pub)
    if issn:
        source = source_by_issn(issn)
        return source.get("display_name") if source else None
    return None


def _journal_issn(pub) -> str | None:
    """
    Get all ISSNs available and return unique values as pipe delimited string
    Used for publisher and journal_name lookups in OpenAlex
    """
    rules = [
        JsonPathRule("openalex_json", "primary_location.source.issn_l"),
        JsonPathRule("openalex_json", "primary_location.source.issn"),  # list
        JsonPathRule("sulpub_json", "issn"),
        JsonPathRule("dim_json", "issn"),
        JsonPathRule("crossref_json", "ISSN"),  # list
        FuncRule("pubmed_json", _pubmed_issn),
    ]
    all_issns = all(pub, rules=rules)  # type: ignore

    flat_issns = []
    for issn in all_issns:
        if isinstance(issn, list):
            issns = [element for element in issn if validate_issn(element)]
            flat_issns.extend(issns)
        elif validate_issn(issn):
            flat_issns.append(issn)
    unique_issns = sorted(list(set(flat_issns)))
    if unique_issns:
        return piped(unique_issns)
    return None


def validate_issn(issn) -> bool:
    """
    Validate ISSN format (basic check)
    """
    if isinstance(issn, str) is False:
        return False
    if len(issn) != 9:
        return False
    prefix = issn[:4]
    suffix = issn[5:]
    if not (
        prefix.isdigit()
        and (suffix[:-1].isdigit() and (suffix[-1].isdigit() or suffix[-1] == "X"))
    ):
        return False
    return True


def _pubmed_issn(pubmed_json: dict) -> str | None:
    if pubmed_json is None:
        return None

    jsonp = json_path("MedlineCitation.Article.Journal.ISSN")
    issn = jsonp.find(pubmed_json)

    if issn:
        return issn[0].value.get("#text", None)

    return None


def _normalize_type(s: str) -> str:
    return type_mapping.get(s.lower(), s.capitalize())


type_mapping = {
    "autobiography": "Book",
    "book": "Book",
    "book-chapter": "Chapter",
    "book-part": "Chapter",
    "book-section": "Chapter",
    "book-series": "Other",
    "book-set": "Other",
    "clinical conference": "Other",
    "clinical study": "Other",
    "clinical trial": "Other",
    "clinical trial protocol": "Other",
    "clinical trial, phase i": "Other",
    "clinical trial, phase ii": "Other",
    "clinical trial, phase iii": "Other",
    "clinical trial, phase iv": "Other",
    "comment": "Other",
    "component": "Other",
    "comparative study": "Other",
    "consensus development conference": "Other",
    "consensus development conference, nih": "Other",
    "controlled clinical trial": "Other",
    "database": "Other",
    "dataset": "Dataset",
    "dissertation": "Dissertation",
    "edited-book": "Book",
    "english abstract": "Other",
    "equivalence trial": "Other",
    "evaluation study": "Other",
    "guideline": "Other",
    "historical article": "Article",
    "interactive tutorial": "Other",
    "journal": "Other",
    "journal article": "Article",
    "journal-article": "Article",
    "journal-issue": "Other",
    "legal case": "Other",
    "meta-analysis": "Article",
    "monograph": "Book",
    "multicenter study": "Other",
    "network meta-analysis": "Article",
    "observational study": "Other",
    "other": "Other",
    "overall": "Other",
    "portrait": "Other",
    "posted-content": "Other",
    "practice guideline": "Other",
    "pragmatic clinical trial": "Other",
    "proceedings": "Other",
    "proceedings-article": "Article",
    "randomized controlled trial": "Other",
    "reference-book": "Other",
    "reference-entry": "Other",
    "report": "Other",
    "report-component": "Other",
    "report-series": "Other",
    "research support, american recovery and reinvestment act": "Other",
    "research support, n.i.h., extramural": "Other",
    "research support, n.i.h., intramural": "Other",
    "research support, non-u.s. gov't": "Other",
    "research support, u.s. gov't, non-p.h.s.": "Other",
    "research support, u.s. gov't, p.h.s.": "Other",
    "scoping review": "Article",
    "standard": "Other",
    "systematic review": "Article",
    "technical report": "Other",
    "twin study": "Other",
    "validation study": "Other",
    "video-audio media": "Other",
    "webcast": "Other",
    "abstract": "Other",
    "address": "Other",
    "art and literature": "Other",
    "article": "Article",
    "bibliography": "Other",
    "biography": "Book",
    "case reports": "Other",
    "casestudy": "Other",
    "chapter": "Chapter",
    "congress": "Other",
    "correction": "Correction/Retraction",
    "data paper": "Article",
    "data set": "Dataset",
    "data study": "Other",
    "dictionary": "Other",
    "early access": "Article",
    "editorial": "Editorial Material ",
    "editorial material": "Editorial Material ",
    "erratum": "Correction/Retraction",
    "expression of concern": "Correction/Retraction",
    "festschrift": "Book",
    "inbook": "Chapter",
    "inproceedings": "Article",
    "interview": "Other",
    "introductory journal article": "Other",
    "item withdrawal": "Correction/Retraction",
    "lecture": "Other",
    "letter": "Other",
    "libguides": "Other",
    "meeting": "Other",
    "news": "Other",
    "otherpaper": "Other",
    "paratext": "Other",
    "patient education handout": "Other",
    "peer-review": "Other",
    "personal narrative": "Other",
    "preprint": "Preprint",
    "proceeding": "Article",
    "publication with expression of concern": "Correction/Retraction",
    "published erratum": "Correction/Retraction",
    "retracted publication": "Correction/Retraction",
    "retraction": "Correction/Retraction",
    "retraction notice": "Correction/Retraction",
    "review": "Article",
    "seminar": "Other",
    "supplementary-materials": "Other",
    "technicalreport": "Other",
    "withdrawn publication": "Correction/Retraction",
    "workingpaper": "Other",
}
