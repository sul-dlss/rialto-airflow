from .utils import JsonPathRule, first


def pub_year(pub):
    """
    Get the pub_year from dimensions, openalex, wos and then sul-pub
    """
    return first(
        pub,
        rules=[
            JsonPathRule("dim_json", "year", is_valid_year=True),
            JsonPathRule("openalex_json", "publication_year", is_valid_year=True),
            JsonPathRule(
                "wos_json", "static_data.summary.pub_info.pubyear", is_valid_year=True
            ),
            JsonPathRule("sulpub_json", "year", is_valid_year=True),
            JsonPathRule("sulpub_json", "journal.year", is_valid_year=True),
        ],
    )
