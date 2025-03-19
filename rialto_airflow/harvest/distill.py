import logging
from dataclasses import dataclass
from typing import Callable, Optional

import jsonpath_ng
from sqlalchemy import select, update

from rialto_airflow.database import Publication, get_session
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.apc import get_apc


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
            }

            # pub_year in cols is needed to determine the apc
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


def _title(pub):
    """
    Get the title from sulpub, dimensions, openalex, then wos.
    """
    return _first(
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
    Get the pub_year from sulpub, dimensions, openalex and then wos.
    """
    return _first(
        pub,
        rules=[
            JsonPathRule("sulpub_json", "year", is_int=True),
            JsonPathRule("dim_json", "year", is_int=True),
            JsonPathRule("openalex_json", "publication_year", is_int=True),
            JsonPathRule(
                "wos_json", "static_data.summary.pub_info.pubyear", is_int=True
            ),
        ],
    )


def _open_access(pub):
    """
    Get the _open_access value from openalex and then dimensions.
    """
    return _first(
        pub,
        rules=[
            JsonPathRule("openalex_json", "open_access.oa_status"),
            FuncRule("dim_json", _open_access_dim),
        ],
    )


def _apc(pub, context):
    """
    Get the APC cost from one place in the openalex data , an external dataset, or another place in
    OpenAlex data.
    """
    # https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/CR1MMV
    return _first(
        pub,
        rules=[
            JsonPathRule("openalex_json", "apc_paid.value_usd"),
            FuncRule("dim_json", _apc_oa_dataset, context),
            JsonPathRule("openalex_json", "apc_list.value_usd"),
        ],
    )


#
# Helper functions to extract from specific JSON data structures that
# aren't workable using JSON Path by itself.
#


def _wos_title(wos_json):
    jsonp = jsonpath_ng.parse("static_data.summary.titles[*].title[*]")
    for title in jsonp.find(wos_json):
        if isinstance(title.value, dict) and title.value.get("type") == "item":
            return title.value.get("content")

    return None


def _open_access_dim(dim_json):
    """
    Get the open access value, but ignore "oa_all"
    """
    jsonp = jsonpath_ng.parse("open_access[*]")
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


#
# Classes and functions for representing matching rules and how to apply them.
# TODO: Maybe these should be in a separate file/module?
#


@dataclass
class JsonPathRule:
    col: str  # the JSONB column name to apply the rule to
    matcher: str  # a JSON Path to evaluate against the JSON
    is_int: bool = False


@dataclass
class FuncRule:
    col: str  # the JSONB column name to pass to a function
    matcher: Callable  # a function to pass the JSON to
    context: Optional[dict] = (
        None  # an optional dictionary of content for use in the matcher
    )


Rules = list[JsonPathRule | FuncRule]


def _first(pub, rules: Rules) -> Optional[str | int]:
    """
    Provide a Publication and a list of rules and return the result of the first rule that matches.
    """
    for rule in rules:
        # get the appropriate bit of json to analyze
        data = getattr(pub, rule.col)

        # if the rule is a string use it as a jsonpath
        if isinstance(rule.matcher, str):
            jpath = jsonpath_ng.parse(rule.matcher)
            results = jpath.find(data)
            if len(results) > 0:
                value = results[0].value
                if rule.is_int:
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        # continue matching if the rule wants an int but we don't have one
                        logging.warn(f'got "{value}" instead of int')
                else:
                    return value

        # if the rule is a function pass it the json, and optional context
        elif callable(rule.matcher):
            if rule.context is not None:
                result = rule.matcher(data, rule.context)
            else:
                result = rule.matcher(data)

            if result is not None:
                return result

    return None
