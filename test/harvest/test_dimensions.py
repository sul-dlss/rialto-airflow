import logging
import os
import pytest

import dotenv
import dimcli

from rialto_airflow.harvest import dimensions
from rialto_airflow.schema.harvest import Publication

from test.utils import num_jsonl_objects, num_log_record_matches

dotenv.load_dotenv()


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


def test_publications_from_orcid():
    pubs = list(dimensions.publications_from_orcid("0000-0002-2317-1967"))
    assert len(pubs) == 17
    assert "10.1002/emp2.12007" in [pub["doi"] for pub in pubs]


@pytest.fixture
def mock_dimensions_dsl_query_error(monkeypatch):
    """
    Mock our function for fetching publications by orcid from Dimensions
    such that the first call results in a retriable error.
    """

    patched_dsl = dimensions.dsl()  # Dimensions dimcli.Dsl instance lets you query
    original_query_iterative = (
        patched_dsl.query_iterative
    )  # a ref to the real query_iterative function

    # for the first call to query_iterative that does an orcid query on publications,
    # raise a request exception.  for the rest, just call the real query function.
    req_count = 0

    def query_iterative_raise_sometimes_fn(*args, **kwargs):
        nonlocal req_count
        if "search publications where researchers.orcid_id = " not in args[0]:
            return original_query_iterative(*args, **kwargs)

        req_count += 1
        if req_count > 1:
            return original_query_iterative(*args, **kwargs)
        else:
            exception = dimensions.requests.exceptions.RequestException(
                "transient error"
            )
            exception.response = type("MockResponse", (), {"status_code": 429})()
            raise exception

    monkeypatch.setattr(
        patched_dsl, "query_iterative", query_iterative_raise_sometimes_fn
    )
    monkeypatch.setattr(
        dimensions, "dsl", lambda: patched_dsl
    )  # wrapped in a lambda because dimensions.dsl is a fn that returns a dimcli.Dsl instance


def test_query_with_retry(mock_dimensions_dsl_query_error, caplog):
    pubs = list(dimensions.publications_from_orcid("0000-0002-2317-1967"))
    assert len(pubs) == 17
    assert "10.1002/emp2.12007" in [pub["doi"] for pub in pubs]
    assert num_log_record_matches(
        caplog.records,
        logging.WARNING,
        "Dimensions query error retry 1 of 5: transient error",
    )


@pytest.fixture
def mock_dimensions_dsl_query_login_error(monkeypatch):
    """
    Mock our function for fetching publications by orcid from Dimensions
    such that the first call results in an authentication error.
    """

    patched_dsl = dimensions.dsl()  # Dimensions dimcli.Dsl instance lets you query
    original_query_iterative = (
        patched_dsl.query_iterative
    )  # a ref to the real query_iterative function

    # for the first call to query_iterative that does an orcid query on publications,
    # raise a request exception. For the rest, just call the real query function.
    req_count = 0

    def query_iterative_raise_sometimes_fn(*args, **kwargs):
        nonlocal req_count
        if "search publications where researchers.orcid_id = " not in args[0]:
            return original_query_iterative(*args, **kwargs)

        req_count += 1
        if req_count > 1:
            return original_query_iterative(*args, **kwargs)
        else:
            exception = dimensions.requests.exceptions.RequestException("login error")
            exception.response = type("MockResponse", (), {"status_code": 401})()
            raise exception

    monkeypatch.setattr(
        patched_dsl, "query_iterative", query_iterative_raise_sometimes_fn
    )
    monkeypatch.setattr(
        dimensions, "dsl", lambda: patched_dsl
    )  # wrapped in a lambda because dimensions.dsl is a fn that returns a dimcli.Dsl instance


def test_query_with_login_retry(mock_dimensions_dsl_query_login_error, caplog):
    pubs = list(dimensions.publications_from_orcid("0000-0002-2317-1967"))
    assert "10.1002/emp2.12007" in [pub["doi"] for pub in pubs]
    assert num_log_record_matches(
        caplog.records,
        logging.WARNING,
        "Dimensions query error retry 1 of 5: login error",
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


def test_harvest(snapshot, test_session, mock_authors, mock_dimensions):
    # harvest from dimensions
    dimensions.harvest(snapshot)

    # the mocked openalex api returns the same publication for both authors
    assert num_jsonl_objects(snapshot.path / "dimensions.jsonl") == 2

    # make sure a publication is in the database and linked to the author
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert pub.doi == "10.1515/9781503624153", "doi was normalized"

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_doi_exists(
    snapshot, test_session, mock_publication, mock_authors, mock_dimensions
):
    # harvest from dimensions
    dimensions.harvest(snapshot)

    # jsonl file is there and has two lines (one for each author)
    assert num_jsonl_objects(snapshot.path / "dimensions.jsonl") == 2

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.dim_json
        assert pub.dim_json["title"] == "An example title", "dimensions json updated"
        assert pub.sulpub_json == {"sulpub": "data"}, "sulpub data the same"
        assert pub.openalex_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_pub_author_association_exists(
    snapshot,
    test_session,
    mock_publication,
    mock_authors,
    mock_association,
    mock_dimensions,
):
    # harvest from dimensions
    dimensions.harvest(snapshot)

    # jsonl file is there and has two lines (one for each author)
    assert num_jsonl_objects(snapshot.path / "dimensions.jsonl") == 2

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.dim_json
        assert pub.dim_json["title"] == "An example title", "dimensions json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


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


def test_log_message(snapshot, mock_authors, mock_many_dimensions, caplog):
    caplog.set_level(logging.INFO)
    dimensions.harvest(snapshot, limit=50)
    assert "Reached limit of 50 publications stopping" in caplog.text


@pytest.fixture
def mock_no_dim_publication(test_session):
    with test_session.begin() as session:
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
    snapshot, test_session, mock_no_dim_publication, mock_dimensions_doi, caplog
):
    caplog.set_level(logging.INFO)
    dimensions.fill_in(snapshot)

    with test_session.begin() as session:
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

    # adds 1 publication to the jsonl file
    assert num_jsonl_objects(snapshot.path / "dimensions-fillin.jsonl") == 1
    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_dimensions(
    snapshot,
    test_session,
    mock_publication,
    mock_no_dim_publication,
    caplog,
    monkeypatch,
):
    caplog.set_level(logging.INFO)

    # make it look like Dimensions returns no publications by DOI
    monkeypatch.setattr(
        dimensions, "publications_from_dois", lambda *args, **kwargs: []
    )

    dimensions.fill_in(snapshot)
    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.dim_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "dimensions-fillin.jsonl") == 0
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
    snapshot,
    test_session,
    mock_no_dim_publication,
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
    dimensions.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.dim_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "dimensions-fillin.jsonl") == 0
    assert "unable to determine what DOI to update" in caplog.text
    assert "filled in 0 publications" in caplog.text
