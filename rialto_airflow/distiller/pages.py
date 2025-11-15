from .utils import FuncRule, JsonPathRule, first, json_path


def pages(row) -> str | int | list | None:
    return first(
        row,
        rules=[
            FuncRule("openalex_json", _openalex_pages),
            JsonPathRule("dim_json", "pages"),
            JsonPathRule("sulpub_json", "journal.pages"),
        ],
    )


def _openalex_pages(openalex_json: dict) -> str | None:
    start_page = _openalex_start_page(openalex_json)
    end_page = _openalex_end_page(openalex_json)
    if start_page and end_page:
        return f"{start_page}-{end_page}"
    elif start_page:
        return start_page
    return end_page


def _openalex_start_page(openalex_json: dict) -> str | None:
    start_jsonp = json_path("biblio.first_page")
    for start_page in start_jsonp.find(openalex_json):
        return start_page.value
    return None


def _openalex_end_page(openalex_json: dict) -> str | None:
    end_jsonp = json_path("biblio.last_page")
    for end_page in end_jsonp.find(openalex_json):
        return end_page.value
    return None
