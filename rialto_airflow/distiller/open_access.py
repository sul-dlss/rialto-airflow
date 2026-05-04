from .utils import JsonPathRule, FuncRule, first, json_path
from .types import types


def open_access(pub):
    """
    Get the _open_access value from openalex and then dimensions.
    """

    # preprints do not actually have openaccess types; just return preprint
    if "Preprint" in types(pub):
        return "preprint"

    return first(
        pub,
        rules=[
            FuncRule("dim_json", _dimensions),
            JsonPathRule("openalex_json", "open_access.oa_status"),
        ],
    )


def _dimensions(dim_json: dict) -> str | None:
    """
    Get the open access value, but ignore "oa_all"
    """
    jsonp = json_path("open_access[*]")
    for oa in jsonp.find(dim_json):
        if oa.value and oa.value != "oa_all":
            return oa.value

    return None
