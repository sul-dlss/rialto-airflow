import itertools

from .utils import JsonPathRule, FuncRule, all, first, json_path
from rialto_airflow.utils import normalize_orcid


def author_list_orcids(row) -> list[str]:
    """
    Get a pipe delimited list of all the author names.
    """
    orcids = all(
        row,
        rules=[
            JsonPathRule(
                "openalex_json", "authorships[*].author.orcid", return_list=True
            ),
            JsonPathRule("dim_json", "authors[*].orcid[*]", return_list=True),
            FuncRule("pubmed_json", _pubmed_orcids),
            JsonPathRule(
                "wos_json",
                "static_data.summary.names.name[*].orcid_id",
                return_list=True,
            ),
            JsonPathRule(
                "wos_json",
                "static_data.summary.names.name.orcid_id",
                return_list=True,
            ),
            JsonPathRule("crossref_json", "author[*].ORCID", return_list=True),
        ],
    )

    # restructure a list of lists into a flat list of strings
    orcids = list(itertools.chain(*orcids))

    orcids = [normalize_orcid(orcid) for orcid in orcids if orcid is not None]

    # unique
    orcids = sorted(list(set(orcids)))

    return orcids


def first_author_orcid(row) -> str | None:
    orcid = first(
        row,
        rules=[
            JsonPathRule("openalex_json", "authorships[0].author.orcid"),
            JsonPathRule("dim_json", "authors[0].orcid[0]"),
            FuncRule("pubmed_json", _pubmed_first_author_orcid),
            JsonPathRule("wos_json", "static_data.summary.names.name[0].orcid_id"),
            JsonPathRule("wos_json", "static_data.summary.names.name.orcid_id"),
            JsonPathRule("crossref_json", "author[0].ORCID"),
        ],
    )

    if orcid is not None:
        orcid = normalize_orcid(orcid)

    return orcid


def last_author_orcid(row) -> str | None:
    orcid = first(
        row,
        rules=[
            JsonPathRule("openalex_json", "authorships[-1].author.orcid"),
            JsonPathRule("dim_json", "authors[-1].orcid[0]"),
            FuncRule("pubmed_json", _pubmed_last_author_orcid),
            JsonPathRule("wos_json", "static_data.summary.names.name[-1].orcid_id"),
            JsonPathRule("wos_json", "static_data.summary.names.name.orcid_id"),
            JsonPathRule("crossref_json", "author[-1].ORCID"),
        ],
    )

    if orcid is not None:
        orcid = normalize_orcid(orcid)

    return orcid


def _pubmed_orcids(row):
    orcids = []
    for result in json_path(
        "MedlineCitation.Article.AuthorList.Author[*].Identifier"
    ).find(row):
        ids = result.value

        # Identifier can be a single dict, or a list of dicts.
        # Ensure that it is a list of dicts.
        if type(ids) is dict:
            ids = [ids]

        for id_ in ids:
            if id_.get("@Source") == "ORCID":
                orcids.append(id_["#text"])

    return orcids


def _pubmed_first_author_orcid(pub) -> str | None:
    return _pubmed_author_orcid(pub, pos=0)


def _pubmed_last_author_orcid(pub) -> str | None:
    return _pubmed_author_orcid(pub, pos=-1)


def _pubmed_author_orcid(pub, pos: int) -> str | None:
    try:
        results = json_path(
            f"MedlineCitation.Article.AuthorList.Author[{pos}].Identifier"
        ).find(pub)
    except KeyError:
        # sometimes there is a single author object instead of a list of author objects
        # in which case we always return the authors ORCID (if it's there)
        results = json_path(
            "MedlineCitation.Article.AuthorList.Author.Identifier"
        ).find(pub)

    for result in results:
        ids = result.value

        # Identifier can be a single dict or a list of dicts
        if type(ids) is dict:
            ids = [ids]

        for id_ in ids:
            if id_.get("@Source") == "ORCID":
                return id_.get("#text")

    return None
