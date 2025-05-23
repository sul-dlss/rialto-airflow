import os

import dotenv

from rialto_airflow.database import Publication
from rialto_airflow.harvest import sul_pub
from rialto_airflow.snapshot import Snapshot

from test.utils import num_jsonl_objects

dotenv.load_dotenv()

sul_pub_host = os.environ.get("AIRFLOW_VAR_SUL_PUB_HOST")
sul_pub_key = os.environ.get("AIRFLOW_VAR_SUL_PUB_KEY")

no_auth = not (sul_pub_host and sul_pub_key)


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
        },
        {
            "title": "Another title",
            "identifier": [
                {"type": "doi", "id": "https://doi.org/10.1215/0961754X-9809305"}
            ],
            "authorship": [
                {"status": "unknown", "cap_profile_id": "12345"},
                {"status": "unknown", "cap_profile_id": "123456"},
            ],
        },
    ]
}


def test_harvest(tmp_path, test_session, mock_authors, requests_mock):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # make sure the jsonl file looks good
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 2

    # make sure there are publications in the database
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert pub.doi == "10.1515/9781503624153", "doi was normalized"
        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].cap_profile_id == "12345"
        assert pub.authors[1].cap_profile_id == "123456"


def test_harvest_when_doi_exists(
    tmp_path, test_session, mock_publication, mock_authors, requests_mock
):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 2

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
    snapshot = Snapshot.create(tmp_path, "rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 2

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
