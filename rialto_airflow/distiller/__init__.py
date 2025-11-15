from .utils import FuncRule, JsonPathRule, all, first, json_path

from .title import title
from .pub_year import pub_year
from .open_access import open_access
from .types import types
from .publisher import publisher
from .journal_issn import journal_issn
from .journal_name import journal_name
from .apc import apc

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

__all__ = [
    "all",
    "apc",
    "first",
    "FuncRule",
    "journal_issn",
    "journal_name",
    "json_path",
    "JsonPathRule",
    "open_access",
    "pub_year",
    "publisher",
    "title",
    "types",
]
