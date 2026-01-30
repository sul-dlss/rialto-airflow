from .utils import JsonPathRule, first
from .journal_issn import journal_issn
from rialto_airflow.harvest.openalex import source_by_issn


def publisher(pub):
    """
    Get the publisher from OpenAlex json if it exists
    """
    publisher = first(
        pub,
        rules=[
            JsonPathRule(
                "openalex_json", "primary_location.source.host_organization_name"
            ),
        ],
    )

    if publisher:
        return publisher
    else:
        return _publisher_by_issn(pub)


def _publisher_by_issn(row) -> str | None:
    """
    # look up publisher in OpenAlex by ISSN
    """
    issn = journal_issn(row)
    if issn is None or issn == "":
        return None

    source = source_by_issn(issn)
    return source.get("host_organization_name") if source else None
