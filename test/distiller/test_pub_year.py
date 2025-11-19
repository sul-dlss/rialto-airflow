import logging

from rialto_airflow.schema.harvest import Publication
from rialto_airflow.distiller import pub_year


def test_sulpub(sulpub_json):
    """
    pub_year should come from sul_pub if all others are unavailable
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,
    )

    assert pub_year(pub) == 2020


def test_from_journal_sulpub():
    """
    pub_year should come from sul_pub journal if all others are unavailable and year is not at top level
    """

    sulpub_json_journal_year = {
        "title": "On the dangers of stochastic parrots (sulpub journal year)",
        "issn": "3333-3333",
        "journal": {
            "name": "Journal of Smart Things That People Write",
            "year": "2013",
            "issue": "1",
        },
    }

    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json_journal_year,
    )

    assert pub_year(pub) == 2013


def test_dim_future(sulpub_json, openalex_json, wos_json):
    """
    pub_year should not come from dimensions since it is in the future (get from another source)
    """

    dim_json_future_year = {
        "title": "On the dangers of stochastic parrots (dim future)",
        "year": "2105",
        "type": "article",
    }

    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,
        dim_json=dim_json_future_year,
        openalex_json=openalex_json,
        wos_json=wos_json,
    )

    pub_year(pub) == 2022


def test_dim(dim_json, openalex_json, wos_json, sulpub_json):
    """
    pub_year should come from dimensions before openalex, wos, and sulpub
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )

    assert pub_year(pub) == 2021


def test_openalex(openalex_json, wos_json, sulpub_json):
    """
    pub_year should come from openalex before wos and sulpub
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )

    assert pub_year(pub) == 2022


def test_wos(wos_json, sulpub_json):
    """
    pub_year should come from wos before sulpub
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )

    assert pub_year(pub) == 2023


def test_none():
    """
    no pub_year shouldn't be a problem
    """
    pub = Publication(doi="10.1515/9781503624153")

    assert pub_year(pub) is None


def test_non_int_year():
    """
    Test that non-integer years don't cause a problem.
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json={"year": "nope"},
        dim_json={"year": None},
    )

    assert pub_year(pub) is None


def test_non_int_year_fallback(caplog):
    """
    Test that higher priority non-integer year doesn't prevent a year coming from another source.
    """
    caplog.set_level(logging.DEBUG)

    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={"year": "nope"},
        openalex_json={"publication_year": 2022},
    )

    assert pub_year(pub) == 2022
    assert (
        "could not cast 'nope' to a non-future year; jpath=year; data={'year': 'nope'}"
        in caplog.text
    )
