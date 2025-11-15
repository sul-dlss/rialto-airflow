from typing import Any

from .utils import JsonPathRule, FuncRule, first, json_path
from rialto_airflow.utils import join_keys


def author_list_names(row) -> list[Any]:
    """
    Get a list of all the author names.
    """
    names = first(
        row,
        rules=[
            JsonPathRule(
                "openalex_json", "authorships[*].author.display_name", return_list=True
            ),
            FuncRule("dim_json", _dim_author_list_names),
            FuncRule("pubmed_json", _pubmed_author_list_names),
            JsonPathRule(
                "wos_json",
                "static_data.summary.names.name[*].display_name",
                return_list=True,
            ),
            JsonPathRule(
                "wos_json",
                "static_data.summary.names.name.display_name",
                return_list=True,
            ),
            FuncRule("crossref_json", _crossref_author_list_names),
            FuncRule("sulpub_json", _sulpub_author_list_names),
        ],
    )

    # the result of first could be None, a string or a list of values
    # but we always need to return a list
    if names is None:
        return []
    elif type(names) is list:
        return names
    else:
        # package up single value in a list
        return [names]


def first_author_name(row) -> str | None:
    names = author_list_names(row)
    return names[0] if names is not None and len(names) > 0 else None


def last_author_name(row) -> str | None:
    names = author_list_names(row)
    return names[-1] if names is not None and len(names) > 0 else None


def _dim_author_list_names(row) -> list[str]:
    if row is None:
        return []

    return [
        match.value["first_name"] + " " + match.value["last_name"]
        for match in json_path("authors[*]").find(row)
    ]


def _pubmed_author_list_names(row) -> list[str]:
    if row is None:
        return []

    return [
        join_keys(match.value, "ForeName", "LastName")
        for match in json_path("MedlineCitation.Article.AuthorList.Author[*]").find(row)
    ]


def _crossref_author_list_names(row) -> list[str]:
    if row is None:
        return []

    return [
        join_keys(match.value, "given", "family")
        for match in json_path("author[*]").find(row)
    ]


def _sulpub_author_list_names(row) -> list[str]:
    """
    Turn names like "Stanford, L. D." into "L. D. Stanford"
    """
    if row is None:
        return []

    names = []
    for match in json_path("author[*].name").find(row):
        parts = [s.strip() for s in match.value.split(",")]
        names.append(" ".join(parts[1:] + [parts[0]]))

    return names
