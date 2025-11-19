from rialto_airflow.schema.harvest import Publication
from rialto_airflow.harvest.distill import open_access


def test_openalex(openalex_json, dim_json):
    """
    open_access should come from openalex first
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json=openalex_json,
        dim_json=dim_json,
    )

    assert open_access(pub) == "gold"


def test_dimensions(dim_json):
    """
    open_access should come from dimensions if it is unavailable in openalex
    """
    pub = Publication(doi="10.1515/9781503624153", dim_json=dim_json)

    assert open_access(pub) == "green"


def test_open_access_null(dim_json):
    """
    dimensions is still used when there is an empty in openalex open_access
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={"open_access": []},
        dim_json=dim_json,
    )

    assert open_access(pub) == "green"


def test_prefer_openalex(sulpub_json, dim_json, openalex_json, wos_json):
    """
    dimensions is still used when there is an empty in openalex open_access
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
    )
    assert open_access(pub) == "gold", "prefer open alex"


def test_fallback_to_dimensions(dim_json, wos_json, sulpub_json):
    pub = Publication(
        doi="10.1515/9781503624153-2",
        sulpub_json=sulpub_json,
        dim_json=dim_json,
        wos_json=wos_json,
    )
    assert open_access(pub) == "green", "fell back to dimensions"
