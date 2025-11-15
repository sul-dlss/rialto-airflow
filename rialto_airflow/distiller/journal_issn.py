from .utils import JsonPathRule, FuncRule, all, json_path
from rialto_airflow.utils import piped


def journal_issn(pub) -> str | None:
    """
    Get all ISSNs available and return unique values as pipe delimited string
    Used for publisher and journal_name lookups in OpenAlex
    """
    rules = [
        JsonPathRule("openalex_json", "primary_location.source.issn_l"),
        JsonPathRule("openalex_json", "primary_location.source.issn"),  # list
        JsonPathRule("sulpub_json", "issn"),
        JsonPathRule("dim_json", "issn"),
        JsonPathRule("crossref_json", "ISSN"),  # list
        FuncRule("pubmed_json", _pubmed_issn),
    ]
    all_issns = all(pub, rules=rules)  # type: ignore

    flat_issns = []
    for issn in all_issns:
        if isinstance(issn, list):
            issns = [element for element in issn if _validate_issn(element)]
            flat_issns.extend(issns)
        elif _validate_issn(issn):
            flat_issns.append(issn)
    unique_issns = sorted(list(set(flat_issns)))
    if unique_issns:
        return piped(unique_issns)
    return None


def _validate_issn(issn) -> bool:
    """
    Validate ISSN format (basic check)
    """
    if isinstance(issn, str) is False:
        return False
    if len(issn) != 9:
        return False
    prefix = issn[:4]
    suffix = issn[5:]
    if not (
        prefix.isdigit()
        and (suffix[:-1].isdigit() and (suffix[-1].isdigit() or suffix[-1] == "X"))
    ):
        return False
    return True


def _pubmed_issn(pubmed_json: dict) -> str | None:
    if pubmed_json is None:
        return None

    jsonp = json_path("MedlineCitation.Article.Journal.ISSN")
    issn = jsonp.find(pubmed_json)

    if issn:
        return issn[0].value.get("#text", None)

    return None
