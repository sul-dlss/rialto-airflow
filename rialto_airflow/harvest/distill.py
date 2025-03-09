import logging
from dataclasses import dataclass
from typing import Callable, Optional

import jsonpath_ng
from sqlalchemy import select, update

from rialto_airflow.database import Publication, get_session
from rialto_airflow.snapshot import Snapshot


@dataclass
class JsonPathRule:
    col: str  # the JSONB column name to apply the rule to
    matcher: str  # a JSON Path to evaluate against the JSON


@dataclass
class FuncRule:
    col: str  # the JSONB column name to pass to a function
    matcher: Callable  # a function to pass the JSON to


Rules = list[JsonPathRule | FuncRule]


def distill(snapshot: Snapshot) -> None:
    """
    Walk through all publications in the database and set the title, pub_year,
    open_access columns using the harvested metadata.
    """
    with get_session(snapshot.database_name).begin() as select_session:
        # iterate through publictions 100 at a time
        stmt = select(Publication).execution_options(yield_per=100)  # type: ignore

        for pos, row in enumerate(select_session.execute(stmt)):
            if pos % 100 == 0:
                logging.info(f"processed {pos} publications")

            pub = row[0]

            # populate new publication columns
            cols = {
                "title": _title(pub),
                "pub_year": _pub_year(pub),
                "open_access": _open_access(pub),
                # These are TBD:
                # "apc":                _apc(pub),
                # "funders":            _funders(pub),
                # "federally_funded":   _federally_funded(pub)
            }

            # persist the new columns to the database
            with get_session(snapshot.database_name).begin() as update_session:
                update_stmt = (
                    update(Publication)  # type: ignore
                    .where(Publication.id == pub.id)
                    .values(cols)
                )
                update_session.execute(update_stmt)


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
    return _first_int(
        pub,
        rules=[
            JsonPathRule("sulpub_json", "year"),
            JsonPathRule("dim_json", "year"),
            JsonPathRule("openalex_json", "publication_year"),
            JsonPathRule("wos_json", "static_data.summary.pub_info.pubyear"),
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


def _first(pub, rules: Rules) -> Optional[str]:
    """
    Return the first rule match for the publication as a str.
    """
    for rule in rules:
        # get the appropriate bit of json to analyze
        data = getattr(pub, rule.col)

        # if the rule is a string it's a jsonpath
        if isinstance(rule.matcher, str):
            jpath = jsonpath_ng.parse(rule.matcher)
            results = jpath.find(data)
            if len(results) > 0:
                return results[0].value

        # if the rule is a function pass it the json
        elif callable(rule.matcher):
            result = rule.matcher(data)
            if result is not None:
                return result

    return None


def _first_int(pub, rules: Rules) -> Optional[int]:
    """
    Return the first rule match for the publication as an int.
    """
    result = _first(pub, rules)
    if result:
        return int(result)
    else:
        return None


# These are custom functions to work with specific JSON data structures that
# aren't workable using JSON Path alone.


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


# TODO: We need to distill other values but this will involve additional work to
# get appropriate data:
#
# - apc
# - funders
# - federally funded
#
# See: https://docs.google.com/document/d/1WojtunkzNtidF2JW4ZLcSClbxajMkH36eUdRIl9tk0E/edit?tab=t.0#heading=h.8lc1fr3onylw
