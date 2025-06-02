import logging

from rialto_airflow.distiller import first, JsonPathRule, FuncRule
from rialto_airflow.harvest.pubmed import get_identifier


def _journal(row):
    """
    Get the journal name in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "journal.title"),
            FuncRule("openalex_json", _openalex_journal),
            FuncRule("wos_json", _wos_journal),
            JsonPathRule("sulpub_json", "journal.name"),
        ],
    )


def _openalex_journal(openalex_json):
    if not openalex_json:
        return None

    try:
        primary_location = openalex_json["primary_location"]
        if (
            primary_location is not None
            and primary_location.get("source")
            and primary_location["source"]["type"] == "journal"
        ):
            return primary_location["source"]["display_name"]
    except (KeyError, TypeError):
        logging.warning("[title] OpenAlex JSON does not contain primary location")
        return None

    return None


def _wos_journal(wos_json):
    if not wos_json:
        return None

    try:
        for title in wos_json["static_data"]["summary"]["titles"]["title"]:
            if title.get("type") and title["type"] == "source":
                return title["content"]
    except (KeyError, TypeError):
        logging.warning("[title] WOS JSON does not contain journal title")
        return None

    return None


def _issue(row):
    """
    Get the journal issue in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "issue"),
            JsonPathRule("openalex_json", "biblio.issue"),
            JsonPathRule("wos_json", "static_data.summary.pub_info.issue"),
            JsonPathRule("sulpub_json", "journal.issue"),
        ],
    )


def _mesh(row):
    """
    Get the mesh in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            FuncRule("dim_json", _dim_mesh),
            FuncRule("openalex_json", _openalex_mesh),
        ],
    )


def _dim_mesh(dim_json):
    if not dim_json or not dim_json.get("mesh_terms"):
        return None

    try:
        return "|".join(list(map(lambda x: x, dim_json["mesh_terms"])))
    except (KeyError, TypeError):
        logging.warning("[mesh] Dimensions JSON does not contain mesh terms")
        return None


def _openalex_mesh(openalex_json):
    if not openalex_json or not openalex_json.get("mesh"):
        return None

    try:
        return "|".join(
            list(map(lambda x: x["descriptor_name"], openalex_json["mesh"]))
        )
    except (KeyError, TypeError):
        logging.warning("[mesh] OpenAlex JSON does not contain mesh terms")
        return None


def _pages(row):
    """
    Get the pages in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "pages"),
            FuncRule("openalex_json", _openalex_pages),
            FuncRule("wos_json", _wos_pages),
            JsonPathRule("sulpub_json", "journal.pages"),
        ],
    )


def _openalex_pages(openalex_json):
    if not openalex_json or not openalex_json.get("biblio"):
        return None

    try:
        return f"{openalex_json['biblio']['first_page']}-{openalex_json['biblio']['last_page']}"
    except (KeyError, TypeError):
        logging.warning("[pages] OpenAlex JSON does not contain pages")
        return None


def _wos_pages(wos_json):
    if not wos_json or not wos_json.get("static_data", {}).get("summary", {}).get(
        "pub_info", {}
    ).get("page", {}):
        return None

    try:
        return f"{wos_json['static_data']['summary']['pub_info']['page']['begin']}-{wos_json['static_data']['summary']['pub_info']['page']['end']}"
    except (KeyError, TypeError):
        logging.warning("[pages] WOS JSON does not contain page begin/end")
        return None


def _pmid(row):
    """
    Get the pmid in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            FuncRule("pubmed_json", _pubmed_pmid),
            JsonPathRule("dim_json", "pmid"),
            JsonPathRule("openalex_json", "ids.pmid"),
            JsonPathRule("sulpub_json", "pmid"),
            FuncRule("wos_json", _wos_pmid),
        ],
    )


def _pubmed_pmid(pubmed_json):
    if not pubmed_json:
        return None

    return get_identifier(pubmed_json, "pubmed")


def _wos_pmid(wos_json):
    if not wos_json:
        return None

    try:
        for identifier in wos_json["dynamic_data"]["cluster_related"]["identifiers"][
            "identifier"
        ]:
            # sometimes wos represents an identifier as a string instead of dict
            if isinstance(identifier, dict) and identifier["type"] == "pmid":
                return identifier["value"]
    except (KeyError, TypeError):
        logging.warning("[pmid] WOS JSON does not contain pmid")
        return None

    return None


def _url(row):
    """
    Get the url in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "linkout"),
            FuncRule("openalex_json", _openalex_url),
        ],
    )


def _openalex_url(openalex_json):
    if not openalex_json:
        return None

    try:
        for location in openalex_json["locations"]:
            if "url" in location:
                return location["url"]
            if "pdf_url" in location:
                return location["pdf_url"]
    except KeyError:
        logging.warning("[url] OpenAlex JSON does not contain a url")
        return None

    return None


def _volume(row):
    """
    Get the volume in order of preference from the sources.
    """
    return first(
        row,
        rules=[
            JsonPathRule("dim_json", "volume"),
            JsonPathRule("openalex_json", "biblio.volume"),
            JsonPathRule("wos_json", "static_data.summary.pub_info.vol"),
            JsonPathRule("sulpub_json", "journal.volume"),
        ],
    )
