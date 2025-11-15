from .utils import JsonPathRule, FuncRule, first, json_path


def open_access(pub):
    """
    Get the _open_access value from openalex and then dimensions.
    """
    return first(
        pub,
        rules=[
            JsonPathRule("openalex_json", "open_access.oa_status"),
            FuncRule("dim_json", _dimensions),
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
