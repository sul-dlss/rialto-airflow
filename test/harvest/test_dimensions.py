import json
import logging
import pytest

import dotenv

from rialto_airflow.harvest import dimensions
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.database import Publication

from test.utils import num_jsonl_objects

dotenv.load_dotenv()


def test_publications_from_dois():
    # use batch_size=1 to test paging for two DOIs
    pubs = list(
        dimensions.publications_from_dois(
            ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], batch_size=1
        )
    )
    assert len(pubs) == 2
    assert len(pubs[0].keys()) == 74, "first publication has 74 columns"
    assert len(pubs[1].keys()) == 74, "second publication has 74 columns"


def test_publication_fields():
    fields = dimensions.publication_fields()
    assert len(fields) == 74
    assert "title" in fields


def test_orcid_publications():
    pubs = list(dimensions.orcid_publications("0000-0002-2317-1967"))
    assert len(pubs) == 16
    assert "10.1002/emp2.12007" in [pub["doi"] for pub in pubs]
    assert len(pubs[0].keys()) == 74, "first publication has 74 columns"
    assert len(pubs[1].keys()) == 74, "second publication has 74 columns"


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

    monkeypatch.setattr(dimensions, "orcid_publications", f)


def test_harvest(tmp_path, test_session, mock_authors, mock_dimensions):
    # harvest from dimensions
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
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
    tmp_path, test_session, mock_publication, mock_authors, mock_dimensions
):
    # harvest from dimensions
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
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
    tmp_path,
    test_session,
    mock_publication,
    mock_authors,
    mock_association,
    mock_dimensions,
):
    # harvest from dimensions
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
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

    monkeypatch.setattr(dimensions, "orcid_publications", f)


def test_log_message(tmp_path, mock_authors, mock_many_dimensions, caplog):
    caplog.set_level(logging.INFO)
    snapshot = Snapshot(tmp_path, "rialto_test")
    dimensions.harvest(snapshot, limit=50)
    assert "Reached limit of 50 publications stopping" in caplog.text


def mock_jsonl(path):
    """
    Mock the existing jsonl file for Dimensions
    """
    records = [
        {
            "doi": "10.1515/9781503624150",
            "title": "An example title",
            "publication_year": 1891,
        },
        {
            "doi": "10.1515/9781503624151",
            "title": "Another example title",
            "publication_year": 1892,
        },
    ]
    with open(path, "w") as f:
        for record in records:
            f.write(f"{json.dumps(record)}\n")


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
    tmp_path, test_session, mock_no_dim_publication, mock_dimensions_doi, caplog
):
    caplog.set_level(logging.INFO)
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    # set up a pre-existing jsonl file
    jsonl_file = snapshot.path / "dimensions.jsonl"
    mock_jsonl(jsonl_file)

    dimensions.fill_in(snapshot, jsonl_file)

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
    assert num_jsonl_objects(snapshot.path / "dimensions.jsonl") == 3
    assert "filled in 1 publications" in caplog.text


@pytest.fixture
def mock_dimensions_not_found(monkeypatch):
    """
    Mock our function for fetching publications by DOI from Dimensions.
    """

    def f(*args, **kwargs):
        raise StopIteration

    monkeypatch.setattr(dimensions, "publications_from_dois", f)


def test_fill_in_no_doi(
    tmp_path,
    test_session,
    mock_publication,
    mock_no_dim_publication,
    mock_dimensions_not_found,
    caplog,
    monkeypatch,
):
    caplog.set_level(logging.INFO)
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    # set up a pre-existing jsonl file
    jsonl_file = snapshot.path / "dimensions.jsonl"
    mock_jsonl(jsonl_file)
    assert num_jsonl_objects(snapshot.path / "dimensions.jsonl") == 2

    dimensions.fill_in(snapshot, jsonl_file)
    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.dim_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "dimensions.jsonl") == 2
    assert "filled in 0 publications" in caplog.text
    assert "No data found for 10.1515/9781503624199" in caplog.text
