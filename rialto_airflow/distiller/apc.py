from .utils import JsonPathRule, FuncRule, first
from rialto_airflow.apc import get_apc


def apc(pub, context):
    """
    Get the APC cost from one place in the openalex data, an external dataset, or another place in
    OpenAlex data.
    """
    # https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/CR1MMV
    first_match_apc = first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json", "apc_paid.value_usd", only_positive_number=True
            ),
            FuncRule("dim_json", _apc_oa_dataset, context),
            JsonPathRule(
                "openalex_json", "apc_list.value_usd", only_positive_number=True
            ),
        ],
    )
    # non-OA publications should not have an APC charge recorded
    oa = (context.get("open_access") or "").lower()
    is_int = isinstance(first_match_apc, int)

    if is_int and oa == "closed":
        return 0
    elif is_int:
        return first_match_apc
    elif oa == "diamond":
        return 0
    elif oa == "gold":
        return 2450
    elif oa == "hybrid":
        return 3600
    else:
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
