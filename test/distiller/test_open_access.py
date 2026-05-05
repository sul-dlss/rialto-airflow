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


def test_preprint_oa_status():
    """
    If the publication type is Preprint, open_access should return 'preprint'
    """
    # OpenAlex preprint
    pub_oa = Publication(
        openalex_json={"type": "preprint", "open_access": {"oa_status": "gold"}}
    )
    assert open_access(pub_oa) == "preprint"

    # Dimensions preprint
    pub_dim = Publication(
        dim_json={"type": "preprint", "open_access": ["oa_all", "closed"]}
    )
    assert open_access(pub_dim) == "preprint"

    # Both present, but type is preprint
    pub_both = Publication(
        dim_json={"type": "preprint", "open_access": ["oa_all", "green"]},
        openalex_json={"type": "preprint", "open_access": {"oa_status": "gold"}},
    )
    assert open_access(pub_both) == "preprint"
