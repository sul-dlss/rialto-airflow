import logging
import os

import pytest

from rialto_airflow.harvest_incremental import altmetric
from rialto_airflow.schema.rialto import Publication

ignore_livetest = os.environ.get("AIRFLOW_VAR_ALTMETRIC_KEY") is None


@pytest.fixture
def mock_rialto_db_name(monkeypatch):
    monkeypatch.setattr(altmetric, "RIALTO_DB_NAME", "rialto_incremental_test")


@pytest.mark.skipif(
    ignore_livetest, reason="Skipping since there is no Altmetric key for a live test"
)
def test_doi():
    """
    Test that we can fetch information by DOI.
    """
    data = altmetric.get_by_doi("10.1038/s41591-026-04297-7")

    # ensure the basic shape of the JSON data assuming we care about news articles
    assert data is not None
    assert "posts" in data
    assert "news" in data["posts"]
    assert len(data["posts"]["news"]) > 0
    assert "title" in data["posts"]["news"][0]
    assert "url" in data["posts"]["news"][0]
    assert "author" in data["posts"]["news"][0]
    assert "name" in data["posts"]["news"][0]["author"]


@pytest.mark.skipif(
    ignore_livetest, reason="Skipping since there is no Altmetric key for a live test"
)
def test_no_doi():
    """
    Test that we handle when we can't look up the DOI.
    """
    data = altmetric.get_by_doi("10.1515/9781503624153")
    assert data is None


def test_invalid_doi():
    """
    Test that DOIs that are formatted wrong return as None.
    """
    data = altmetric.get_by_doi("this-aint-a-doi")
    assert data is None


def test_fill_in(
    test_incremental_session,
    mock_incremental_publication,
    mock_rialto_db_name,
    caplog,
    monkeypatch,
    active_harvest_id,
):
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(altmetric, "get_by_doi", lambda _: {"foo": "bar"})

    altmetric.fill_in(active_harvest_id)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub is not None
        assert pub.altmetric_json == {"foo": "bar"}

    assert "filled in 1 publications" in caplog.text


def test_fill_in_none(
    test_incremental_session,
    mock_incremental_publication,
    mock_rialto_db_name,
    caplog,
    monkeypatch,
    active_harvest_id,
):
    """
    Ensure that things still work when the DOI is not found.
    """
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(altmetric, "get_by_doi", lambda _: None)

    altmetric.fill_in(active_harvest_id)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub is not None
        assert pub.altmetric_json is None

    assert "filled in 0 publications" in caplog.text
