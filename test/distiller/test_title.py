from rialto_airflow.schema.harvest import Publication
from rialto_airflow.distiller import title


def test_title_sulpub(sulpub_json, dim_json, openalex_json, wos_json):
    """
    title should come from sulpub before dimensions, openalex and wos_json
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
    )

    assert title(pub) == "On the dangers of stochastic parrots (sulpub)"


def test_title_dim(dim_json, openalex_json, wos_json):
    """
    title should come from dimensions before openalex and wos
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
    )

    assert title(pub) == "On the dangers of stochastic parrots (dim)"


def test_title_openalex(openalex_json, wos_json):
    """
    title should come from openalex before wos
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json=openalex_json,
        wos_json=wos_json,
    )

    assert title(pub) == "On the dangers of stochastic parrots (openalex)"


def test_title_wos(wos_json):
    """
    title should come from wos if all the others are unavailable
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        wos_json=wos_json,
    )

    assert title(pub) == "On the dangers of stochastic parrots (wos)"


def test_title_none():
    """
    Ensure that no title doesn't cause a problem.
    """
    pub = Publication(doi="10.1515/9781503624153")

    assert title(pub) is None


def test_title_booktitle():
    """
    title should come from sulpub's booktitle if it's not available elsewhere
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json={"booktitle": "Gravity's Rainbow"},
    )

    assert title(pub) == "Gravity's Rainbow"
