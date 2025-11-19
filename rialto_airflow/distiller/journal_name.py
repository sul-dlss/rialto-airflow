from .utils import JsonPathRule, first
from .journal_issn import journal_issn
from rialto_airflow.harvest.openalex import source_by_issn


def journal_name(pub) -> str | None:
    # try to get journal name from openalex_json before querying
    openalex_journal_name = first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json",
                "locations[?@.source.type == 'journal'].source.display_name",
            ),
        ],
    )
    if openalex_journal_name:
        # we can assume OpenAlex journal names are strings but mypy cannot
        return str(openalex_journal_name)

    # get ISSN and look up journal name in OpenAlex
    issn = journal_issn(pub)
    if issn:
        source = source_by_issn(issn)
        return source.get("display_name") if source else None
    return None
