from rialto_airflow.schema.harvest import Publication
from rialto_airflow.harvest.distill import open_access


def test_dimensions(dim_json):
    """
    open_access should come from dimensions
    """
    pub = Publication(doi="10.1515/9781503624153", dim_json=dim_json)

    assert open_access(pub) == "green"


def test_open_access_null(dim_json):
    """
    open_access should come from dimensions when openalex is available but has no open_access info
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={"open_access": []},
        dim_json=dim_json,
    )

    assert open_access(pub) == "green"


def test_prefer_dimensions(sulpub_json, dim_json, openalex_json, wos_json):
    """
    dimensions is preferred over openalex when both are available
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
    )
    assert open_access(pub) == "green", "prefer dimensions"


def test_fallback_to_openalex(openalex_json, wos_json, sulpub_json):
    """
    openalex is used when dimensions is not available
    """
    pub = Publication(
        doi="10.1515/9781503624153-2",
        sulpub_json=sulpub_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
    )
    assert open_access(pub) == "gold", "fell back to openalex"


def test_no_open_alex_or_dimensions():
    """
    open_access should be empty if no info is available in dimensions or openalex
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={"open_access": []},
        dim_json={"open_access": []},
    )

    assert open_access(pub) is None
