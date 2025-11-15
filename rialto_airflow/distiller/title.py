from rialto_airflow.distiller import JsonPathRule, FuncRule, first, json_path


def title(pub):
    """
    Get the title from sulpub, dimensions, openalex, then wos.
    """
    return first(
        pub,
        rules=[
            JsonPathRule("sulpub_json", "title"),
            JsonPathRule("sulpub_json", "booktitle"),
            JsonPathRule("dim_json", "title"),
            JsonPathRule("openalex_json", "title"),
            FuncRule("wos_json", _wos_title),
        ],
    )


def _wos_title(wos_json):
    jsonp = json_path("static_data.summary.titles[*].title[*]")
    for title in jsonp.find(wos_json):
        if isinstance(title.value, dict) and title.value.get("type") == "item":
            return title.value.get("content")

    return None
