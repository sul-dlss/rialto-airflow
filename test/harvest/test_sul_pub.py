import json
import os

import dotenv
import pytest
from sqlalchemy import insert

from rialto_airflow.database import Publication, Author, pub_author_association
from rialto_airflow.harvest import sul_pub
from rialto_airflow.snapshot import Snapshot

dotenv.load_dotenv()

sul_pub_host = os.environ.get("AIRFLOW_VAR_SUL_PUB_HOST")
sul_pub_key = os.environ.get("AIRFLOW_VAR_SUL_PUB_KEY")

no_auth = not (sul_pub_host and sul_pub_key)


@pytest.fixture
def mock_authors(test_session):
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Author(
                    sunet="janes",
                    cap_profile_id="12345",
                    orcid="https://orcid.org/0000-0000-0000-0001",
                    first_name="Jane",
                    last_name="Stanford",
                    status=True,
                ),
                Author(
                    sunet="lelands",
                    cap_profile_id="123456",
                    orcid="https://orcid.org/0000-0000-0000-0002",
                    first_name="Leland",
                    last_name="Stanford",
                    status=True,
                ),
            ]
        )


@pytest.fixture
def mock_publication(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            sulpub_json={"sulpub": "data"},
            wos_json={"wos": "data"},
        )
        session.add(pub)
        return pub


@pytest.fixture
def mock_association(test_session, mock_publication, mock_authors):
    with test_session.begin() as session:
        session.execute(
            insert(pub_author_association).values(
                # TODO: should the IDs be looked up in case they aren't always 1?
                publication_id=1,
                author_id=1,
            )
        )


@pytest.mark.skipif(no_auth, reason="no sul_pub key")
def test_publications():
    for pub in sul_pub.publications(sul_pub_host, sul_pub_key, per_page=50, limit=100):
        # all the publications should be approved by at least one author
        for authorship in pub["authorship"]:
            if authorship["status"] == "approved":
                approved = True
        assert approved is True, f"sulpubid={pub['sulpubid']} is marked approved"


response = {
    "records": [
        {
            "title": "An example title",
            "identifier": [{"type": "doi", "id": "10.1515/9781503624153"}],
            "authorship": [
                {"status": "approved", "cap_profile_id": "12345"},
                {"status": "approved", "cap_profile_id": "123456"},
            ],
        }
    ]
}


def test_harvest(tmp_path, test_session, mock_authors, requests_mock):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # make sure the jsonl file looks good
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 1

    # make sure there are publications in the database
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].cap_profile_id == "12345"
        assert pub.authors[1].cap_profile_id == "123456"


def test_harvest_when_doi_exists(
    tmp_path, test_session, mock_publication, mock_authors, requests_mock
):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 1

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.sulpub_json
        assert pub.sulpub_json["title"] == "An example title", "sulpub json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].cap_profile_id == "12345"
        assert pub.authors[1].cap_profile_id == "123456"


def test_harvest_when_author_exists(
    tmp_path,
    test_session,
    mock_publication,
    mock_authors,
    mock_association,
    requests_mock,
):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 1

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.sulpub_json
        assert pub.sulpub_json["title"] == "An example title", "sulpub json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].cap_profile_id == "12345"
        assert pub.authors[1].cap_profile_id == "123456"


def num_jsonl_objects(jsonl_path):
    assert jsonl_path.is_file()
    pubs = [json.loads(line) for line in jsonl_path.open("r")]
    return len(pubs)
