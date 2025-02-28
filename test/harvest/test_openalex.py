import dotenv
import logging
import pytest

from rialto_airflow.harvest import openalex
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.database import Publication

from test.utils import num_jsonl_objects

dotenv.load_dotenv()


def test_orcid_publications():
    """
    This is a live test of OpenAlex API, to make sure paging works properly.
    """
    count = 0
    for pub in openalex.orcid_publications("https://orcid.org/0000-0002-8030-5327"):
        assert pub

        count += 1
        if count >= 400:
            break

    assert count == 400, "found 100 publications"


response = {
    "records": [
        {
            "title": "An example title",
            "identifier": [
                {"type": "doi", "id": "https://doi.org/10.1515/9781503624153"}
            ],
            "authorship": [
                {"status": "approved", "cap_profile_id": "12345"},
                {"status": "approved", "cap_profile_id": "123456"},
            ],
        }
    ]
}


@pytest.fixture
def mock_openalex(monkeypatch):
    """
    Mock our function for fetching publications by orcid from OpenAlex.
    """

    def f(*args, **kwargs):
        yield {
            "doi": "https://doi.org/10.1515/9781503624153",
            "title": "An example title",
            "publication_year": 1891,
        }

    monkeypatch.setattr(openalex, "orcid_publications", f)


def test_harvest(tmp_path, test_session, mock_authors, mock_openalex):
    # harvest from openalex
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    openalex.harvest(snapshot)

    # the mocked openalex api returns the same publication for both authors
    assert num_jsonl_objects(snapshot.path / "openalex.jsonl") == 2

    # make sure a publication is in the database and linked to the author
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert pub.doi == "10.1515/9781503624153", "doi was normalized"

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_doi_exists(
    tmp_path, test_session, mock_publication, mock_authors, mock_openalex
):
    # harvest from openalex
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    openalex.harvest(snapshot)

    # jsonl file is there and has two lines (one for each author)
    assert num_jsonl_objects(snapshot.path / "openalex.jsonl") == 2

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.openalex_json
        assert pub.openalex_json["title"] == "An example title", "sulpub json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_author_exists(
    tmp_path,
    test_session,
    mock_publication,
    mock_authors,
    mock_association,
    mock_openalex,
):
    # harvest from openalex
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    openalex.harvest(snapshot)

    # jsonl file is there and has two lines (one for each author)
    assert num_jsonl_objects(snapshot.path / "openalex.jsonl") == 2

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.openalex_json
        assert pub.openalex_json["title"] == "An example title", "openalex json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


@pytest.fixture
def mock_many_openalex(monkeypatch):
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

    monkeypatch.setattr(openalex, "orcid_publications", f)


def test_log_message(tmp_path, mock_authors, mock_many_openalex, caplog):
    caplog.set_level(logging.INFO)
    snapshot = Snapshot(tmp_path, "rialto_test")
    openalex.harvest(snapshot, limit=50)
    assert "Reached limit of 50 publications stopping" in caplog.text
