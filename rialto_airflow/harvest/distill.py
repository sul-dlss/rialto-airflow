import logging
from typing import Optional

from sqlalchemy import select, update

from rialto_airflow.apc import get_apc
from rialto_airflow.database import get_session
from rialto_airflow.distiller import FuncRule, JsonPathRule, first, json_path
from rialto_airflow.schema.harvest import Publication
from rialto_airflow.snapshot import Snapshot


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
            if count % 100 == 0:
                logging.info(f"processed {count} publications")

            pub = row[0]

            # populate new publication columns
            cols = {
                "title": _title(pub),
                "pub_year": _pub_year(pub),
                "open_access": _open_access(pub),
                "types": _types(pub),
                "publisher": _publisher(pub),
                "academic_council_authored": _academic_council(pub),
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

    # make lowercase and ordered
    return sorted([str(s).lower() for s in types])


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
    Get the publisher from OpenAlex
    """
    return first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json", "primary_location.source.host_organization_name"
            ),
        ],
    )


def _academic_council(row) -> Optional[bool]:
    """
    Get the academic_council value for all authors and return True if any are academic_council
    """
    authors = row.authors

    for author in authors:
        if author.academic_council:
            return True

    return False
