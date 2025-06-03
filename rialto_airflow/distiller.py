import datetime
import logging
from functools import cache
from dataclasses import dataclass
from typing import Callable, Optional

import jsonpath_ng
from sqlalchemy.engine.row import Row  # type: ignore


"""
This module lets you define rules for extracting information from the JSON that
has been collected for a publication. It is useful in situations where you want
to extract a value from multiple locations in the JSON, which are expressed in
order of preference, and you want to get the first one that matches.

The JsonPathRule will look for the result of applying a particular JSON Path to
the JSON. For rules that can't be expressed as a JSON Path you can use FuncRule
that will use the result of calling a given function with the JSON.

For example this will look for a title in sul_pub, Dimensions, Openalex and fall
back to using a function to extract it from the WebOfScience metadata:

    from rialto_airflow.distiller import first, JsonPathRule, FuncRule

    first(
        pub,
        rules=[
            JsonPathRule("sulpub_json", "title"),
            JsonPathRule("dim_json", "title"),
            JsonPathRule("openalex_json", "title"),
            FuncRule("wos_json", _wos_title)
        ]
    )

    def _wos_title(obj):
        ...

Some distilling happens during the harvest DAG, and some during the data
publishing, so it was pulled out into a separate module.

Maybe this could work only with JSON and be agnostic about operating on a
Publication row?

    first([
        JsonPathRule(pub.sulpub_json, "title"),
        JsonPathRule(pub.dim_json, "title"),
        JsonPathRule(pub.openalex_json, "title"),
        FuncRule(pub.wos_json, _wos_title)
    ])

"""


@dataclass
class JsonPathRule:
    """
    A rule for extracting a JsonPath from a given JSON column.

    col: is the name of the column to extract from, e.g. "sul_pub"
    matcher: a JsonPath string
    is_valid_year: the value isn't greater than the current year (optional)
    only_positive_number: the value is a positive number (optional)
    """

    col: str
    matcher: str
    is_valid_year: bool = False
    only_positive_number: bool = False

    # TODO: Could matcher be JSON Path type? That would ensure it parses?


@dataclass
class FuncRule:
    """
    A rule for using a function to extract from a given JSON column. It is
    useful in situations where a JsonPath alone can't be used. The context
    can include additional information to pass with the rule.

    col: is the name of the column to extract from, e.g. "sul_pub"
    matcher: is a function that is passed the JSON object
    context: an optional dictionary of data to use when matching
    """

    col: str
    matcher: Callable
    context: Optional[dict] = None


Rule = JsonPathRule | FuncRule


def first(pub: Row, rules: list[Rule]) -> Optional[str | int]:
    """
    Examines a Publication row using a list of rules and returns the result of the first rule that matches.
    """
    for rule in rules:
        # get the appropriate bit of json to analyze
        data = getattr(pub, rule.col)

        # variable to store the rule match
        result = None

        if isinstance(rule, JsonPathRule):
            result = _jsonpath_match(rule, data)
        elif isinstance(rule, FuncRule):
            result = _func_match(rule, data)
        else:
            raise Exception(
                "unknown rule matcher: should be JsonPath string or function"
            )

        # if a rule matched return it, otherwise we continue to the next rule
        if result is not None:
            return result

    # none of the rules matched, oh well
    return None


def _jsonpath_match(rule: JsonPathRule, data) -> Optional[str | int]:
    jpath = _json_path(rule.matcher)
    results = jpath.find(data)

    if len(results) > 0:
        # note: we currently only examine the first JSON Path match
        result = results[0].value
        if rule.only_positive_number:
            result = _ensure_positive_number(result)
        elif rule.is_valid_year:
            result = _ensure_valid_year(result)
    else:
        result = None

    return result


def _ensure_positive_number(value: str | int) -> Optional[int]:
    result = None
    try:
        result = int(value)
        if result < 0:
            logging.warning(f"got a non-positive number {value}")
            result = None
    except (ValueError, TypeError):
        logging.warning(f'got "{value}" instead of a positive number')

    return result


def _ensure_valid_year(value: str | int) -> Optional[int]:
    result = None
    try:
        result = int(value)
        if result > datetime.datetime.now().year:
            logging.warning(f"got a year {value} that is in the future")
            result = None
    except (ValueError, TypeError):
        logging.warning(f'got "{value}" instead of int')

    return result


def _func_match(rule: FuncRule, data: dict) -> Optional[str | int]:
    if rule.context is not None:
        result = rule.matcher(data, rule.context)
    else:
        result = rule.matcher(data)

    return result


@cache
def _json_path(path):
    return jsonpath_ng.parse(path)
