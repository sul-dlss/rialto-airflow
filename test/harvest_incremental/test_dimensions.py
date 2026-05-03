import datetime
import logging
import os
import pytest
import requests

import dotenv
import dimcli

from rialto_airflow.harvest_incremental import dimensions
from rialto_airflow.schema.rialto import Author, Harvest, Publication

from test.utils import num_log_record_matches

dotenv.load_dotenv()


def _assert_publication_has_two_expected_authors(pub):
    assert len(pub.authors) == 2, "publication has two authors"
    assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
    assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def _mock_dimensions_dsl_query_error(monkeypatch, error_message, status_code):
    """Raise one retriable request error for ORCID publication queries."""
    patched_dsl = dimensions.dsl()
    original_query_iterative = patched_dsl.query_iterative

    req_count = 0

    def query_iterative_raise_sometimes_fn(*args, **kwargs):
        nonlocal req_count
        if "search publications where researchers.orcid_id = " not in args[0]:
            return original_query_iterative(*args, **kwargs)

        req_count += 1
        if req_count > 1:
            return original_query_iterative(*args, **kwargs)

        exception = dimensions.requests.exceptions.RequestException(error_message)
        response = requests.Response()
        response.status_code = status_code
        exception.response = response
        raise exception

    monkeypatch.setattr(
        patched_dsl, "query_iterative", query_iterative_raise_sometimes_fn
    )
    monkeypatch.setattr(dimensions, "dsl", lambda: patched_dsl)


def test_publications_from_dois():
    # use batch_size=1 to test paging for two DOIs
    pubs = list(
        dimensions.publications_from_dois(
            ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], batch_size=1
        )
    )
    assert len(pubs) == 2
    assert pubs[0]["doi"] == "10.48550/arxiv.1706.03762"
    assert len(pubs[0].keys()) == 28, "first publication has 28 columns"
    assert "book_title" in pubs[0].keys()
    assert len(pubs[1].keys()) == 28, "second publication has 28 columns"


def test_publication_fields():
    fields = dimensions.publication_fields()
    assert len(fields) == 18
    assert "basics" in fields
    assert "book" in fields


def test_harvest_passes_previous_harvest_date_to_orcid_query(
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    monkeypatch,
):
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 4, 27, 16, 38, 10),
                finished_at=datetime.datetime(2026, 4, 28, 0, 0, 0),
            )
        )
        # Make sure Authors are similar to those loaded in production, with created_at dates.
        session.query(Author).where(Author.orcid.is_not(None)).update(
            {
                Author.created_at: datetime.datetime(2026, 4, 20, 0, 0, 0),
                Author.updated_at: None,
            }
        )

    harvest_dates = []

    def _capture_harvest_date(orcid, harvest_date=None):
        # capture the harvest_date that is passed to publications_from_orcid so that we can assert on it later
        harvest_dates.append(harvest_date)
        yield from ()

    monkeypatch.setattr(dimensions, "publications_from_orcid", _capture_harvest_date)

    dimensions.harvest()

    assert harvest_dates, (
        "publications_from_orcid is passed the recent finished harvest date"
    )
    assert set(harvest_dates) == {"2026-04-27"}, (
        "formatted previous harvest date passed to publications_from_orcid"
    )


def test_harvest_omits_previous_harvest_date_for_recently_updated_authors(
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    monkeypatch,
):
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 4, 25, 16, 38, 10),
                finished_at=datetime.datetime(2026, 4, 28, 0, 0, 0),
            )
        )
        session.query(Author).where(
            Author.orcid == "https://orcid.org/0000-0000-0000-0001"
        ).update(
            {
                Author.created_at: datetime.datetime(2025, 1, 1, 0, 0, 0),
                Author.updated_at: datetime.datetime(2026, 4, 28, 0, 0, 0),
            }
        )
        session.query(Author).where(
            Author.orcid == "https://orcid.org/0000-0000-0000-0002"
        ).update(
            {
                Author.created_at: datetime.datetime(2025, 1, 20, 0, 0, 0),
                Author.updated_at: datetime.datetime(2025, 1, 1, 0, 0, 0),
            }
        )

    harvest_dates = []

    def _capture_harvest_date(orcid, harvest_date=None):
        harvest_dates.append(harvest_date)
        yield from ()

    monkeypatch.setattr(dimensions, "publications_from_orcid", _capture_harvest_date)

    dimensions.harvest()

    assert harvest_dates, (
        "publications_from_orcid should be called with harvest_date for ORCID authors"
    )
    assert len(harvest_dates) == 2, "two ORCID authors should be harvested"
    assert harvest_dates == [None, "2026-04-25"], (
        "recently updated author should omit previous harvest date while older updated author should keep it"
    )


def test_publications_from_orcid(mock_rialto_db_name):
    pubs = list(
        dimensions.publications_from_orcid(
            "0000-0002-2317-1967",
            harvest_date="2010-04-27",  # using the same date as the previous harvest to get a consistent set of publications
        )
    )
    assert len(pubs) == 17
    assert "10.1002/emp2.12007" in [pub["doi"] for pub in pubs]


def test_publications_from_orcid_omits_previous_harvest_date_when_none_exists(
    test_incremental_session, mock_rialto_db_name, monkeypatch
):
    queries = []

    def mock_query_with_retry(query, retry=5):
        queries.append(query)
        return {"publications": []}

    monkeypatch.setattr(dimensions, "query_with_retry", mock_query_with_retry)
    list(dimensions.publications_from_orcid("0000-0002-2317-1967", harvest_date=None))

    assert "date_inserted >=" not in queries[0]


@pytest.mark.parametrize(
    "error_message,status_code,expected_doi",
    [
        ("transient error", 429, "10.1016/j.annemergmed.2025.05.017"),
        ("login error", 401, "10.1002/emp2.12007"),
    ],
)
def test_query_with_retry_variants(
    caplog, monkeypatch, error_message, status_code, expected_doi
):
    _mock_dimensions_dsl_query_error(monkeypatch, error_message, status_code)
    pubs = list(
        dimensions.publications_from_orcid("0000-0002-2317-1967", harvest_date=None)
    )
    assert expected_doi in [pub["doi"] for pub in pubs]
    assert num_log_record_matches(
        caplog.records,
        logging.WARNING,
        f"Dimensions query error retry 1 of 5: {error_message}",
    )


@pytest.fixture
def mock_dimensions(monkeypatch):
    """
    Mock our function for fetching publications by orcid from Dimensions.
    """

    def f(*args, **kwargs):
        yield {
            "doi": "https://doi.org/10.1515/9781503624153",
            "title": "An example title",
            "type": "article",
            "publication_year": 1891,
        }

    monkeypatch.setattr(dimensions, "publications_from_orcid", f)


def test_harvest(
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    mock_dimensions,
):
    # harvest from dimensions
    dimensions.harvest()

    # make sure a publication is in the database and linked to the author
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert pub.doi == "10.1515/9781503624153", "doi was normalized"
        assert isinstance(pub.dim_harvested, datetime.datetime), (
            "harvest timestamp is a datetime"
        )
        _assert_publication_has_two_expected_authors(pub)


def test_harvest_when_doi_exists(
    test_incremental_session,
    mock_incremental_publication,
    mock_incremental_authors,
    mock_rialto_db_name,
    mock_dimensions,
):
    # harvest from dimensions
    dimensions.harvest()

    # ensure that the existing publication for the DOI was updated
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.dim_json
        assert pub.dim_json["title"] == "An example title", "dimensions json updated"
        assert pub.sulpub_json == {"sulpub": "data"}, "sulpub data the same"
        assert pub.openalex_json is None
        assert isinstance(pub.dim_harvested, datetime.datetime), (
            "harvest timestamp is a datetime"
        )
        _assert_publication_has_two_expected_authors(pub)


def test_harvest_when_pub_author_association_exists(
    test_incremental_session,
    mock_incremental_publication,
    mock_incremental_authors,
    mock_incremental_association,
    mock_rialto_db_name,
    mock_dimensions,
):
    # harvest from dimensions
    dimensions.harvest()

    # ensure that the existing publication for the DOI was updated
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.dim_json
        assert pub.dim_json["title"] == "An example title", "dimensions json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None
        _assert_publication_has_two_expected_authors(pub)


@pytest.fixture
def mock_many_dimensions(monkeypatch):
    """
    Mock our function for fetching publications by orcid from OpenAlex.
    """

    def f(*args, **kwargs):
        for n in range(1, 1000):
            yield {
                "doi": f"https://doi.org/10.1515/{n}",
                "title": "An example title",
                "publication_year": 1891,
            }

    monkeypatch.setattr(dimensions, "publications_from_orcid", f)


def test_log_message(
    mock_incremental_authors,
    mock_rialto_db_name,
    mock_many_dimensions,
    caplog,
):
    caplog.set_level(logging.INFO)
    dimensions.harvest(limit=50)
    assert "Reached limit of 50 publications or authors, stopping" in caplog.text


def test_harvest_stops_when_author_limit_is_exceeded(
    mock_incremental_authors,
    mock_rialto_db_name,
    caplog,
    monkeypatch,
):
    queried_orcids = []

    def _capture_orcid(orcid, harvest_date=None):
        # keep track of ORCIDs queries but don't return any publications (so that we're only testing the author limit)
        queried_orcids.append(orcid)
        yield from ()

    monkeypatch.setattr(dimensions, "publications_from_orcid", _capture_orcid)

    caplog.set_level(logging.INFO)
    dimensions.harvest(limit=1)

    assert queried_orcids == ["https://orcid.org/0000-0000-0000-0001"]
    assert "Reached limit of 1 publications or authors, stopping" in caplog.text


@pytest.fixture
def mock_no_dim_publication(test_incremental_session):
    with test_incremental_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624199",
            sulpub_json={"sulpub": "data"},
            wos_json={"wos": "data"},
        )
        session.add(pub)
        return pub


@pytest.fixture
def mock_dimensions_doi(monkeypatch):
    """
    Mock our function for fetching publications by DOI from Dimensions.
    """

    def f(*args, **kwargs):
        yield {
            "doi": "10.1515/9781503624199",
            "title": "An example title",
            "type": "article",
            "publication_year": 1891,
        }

    monkeypatch.setattr(dimensions, "publications_from_dois", f)


def test_fill_in(
    test_incremental_session,
    mock_no_dim_publication,
    mock_rialto_db_name,
    mock_dimensions_doi,
    caplog,
):
    caplog.set_level(logging.INFO)
    dimensions.fill_in()

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.dim_json == {
            "doi": "10.1515/9781503624199",
            "title": "An example title",
            "type": "article",
            "publication_year": 1891,
        }

    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_dimensions(
    test_incremental_session,
    mock_incremental_publication,
    mock_no_dim_publication,
    mock_rialto_db_name,
    caplog,
    monkeypatch,
):
    caplog.set_level(logging.INFO)

    # make it look like Dimensions returns no publications by DOI
    monkeypatch.setattr(
        dimensions, "publications_from_dois", lambda *args, **kwargs: []
    )

    dimensions.fill_in()
    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.dim_json is None

    assert "filled in 0 publications" in caplog.text


@pytest.mark.skip(reason="this test appears to be inconsistent")
def test_researchers_error():
    """
    The Dimensions API can intermittently throw Service Unavailable errors when we include
    "researchers" in the list of fields that we want to return.

    If this test starts to fail that should be a flag that we can consider adding
    "researchers" back to the list of fields that we query Dimensions for.

    See: https://github.com/digital-science/dimcli/issues/90
    """
    dimcli.login(
        key=os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_KEY"),
        endpoint="https://app.dimensions.ai/api/dsl/v2",
    )

    dsl = dimcli.Dsl()

    q = """
    search publications where doi in ["10.3847/1538-4357/ad9749","10.1103/physrevd.111.042005","10.3847/1538-4357/ad8de0","10.1364/fio.2024.jtu4a.2","10.3847/1538-4357/ad65ce","10.1364/cleo_si.2024.sm1d.3","10.1103/physrevd.110.042001","10.3847/1538-4357/ad3e83","10.3847/2041-8213/ad5beb"]
    return publications [researchers]
    limit 1000
    """

    results = dsl.query(q)
    assert results.errors["query"]["header"] == "Service unavailable"


def test_fill_in_no_doi(
    test_incremental_session,
    mock_no_dim_publication,
    mock_rialto_db_name,
    caplog,
    monkeypatch,
):
    """
    Test that a publication coming back from Dimensions without DOI doesn't
    cause an exception.
    """

    monkeypatch.setattr(
        dimensions,
        "publications_from_dois",
        lambda *args, **kwargs: [{"title": "Example"}],
    )

    caplog.set_level(logging.INFO)
    dimensions.fill_in()

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.dim_json is None

    assert "unable to determine what DOI to update" in caplog.text
    assert "filled in 0 publications" in caplog.text
