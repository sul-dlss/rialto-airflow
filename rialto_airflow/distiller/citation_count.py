from .utils import JsonPathRule, all


def citation_count(row) -> str | int | None:
    """
    Get the citation count from OpenAlex, Dimensions, then WOS.
    """
    counts = all(
        row,
        rules=[
            JsonPathRule("openalex_json", "cited_by_count"),
            JsonPathRule("dim_json", "recent_citations"),
            JsonPathRule(
                "wos_json",
                "dynamic_data.citation_related.tc_list.silo_tc[?@.coll_id == 'WOS'].local_count",
            ),
        ],
    )
    # drop any string or None values
    counts = [count for count in counts if isinstance(count, int)]
    return max(counts) if counts else None
