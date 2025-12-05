from rialto_airflow.utils import piped
from .utils import FuncRule, json_path, first


def grant_ids(pub):
    """
    Get the funder grant award_ids from OpenAlex
    """
    return first(
        pub,
        rules=[
            FuncRule("openalex_json", _oa_grant_awards),
        ],
    )


def _oa_grant_awards(openalex_json):
    jsonp = json_path("awards[*].funder_award_id")
    award_ids = [id.value for id in jsonp.find(openalex_json) if id.value is not None]

    return piped(award_ids) if award_ids else None
