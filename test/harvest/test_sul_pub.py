import os
import logging

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
            "title": "An example title with DOI in top level only",
            "doi": "https://doi.org/10.1515/9781503624153",
            "authorship": [
                {"status": "approved", "cap_profile_id": "12345"},
                {"status": "approved", "cap_profile_id": "123456"},
            ],
        },
        {
            "title": "Another title with no approved authors - will be ignored",
            "sulpubid": "456012",
            "identifier": [
                {"type": "doi", "id": "https://doi.org/10.1215/0961754X-9809305"}
            ],
            "authorship": [
                {"status": "unknown", "cap_profile_id": "12345"},
                {"status": "unknown", "cap_profile_id": "123456"},
            ],
        },
        {
            "title": "Another title that will be harvested with DOI in identifier field only",
            "sulpubid": "123789",
            "identifier": [
                {"type": "doi", "id": "https://doi.org/10.9999/0161754X-9809305"}
            ],
            "authorship": [
                {"status": "approved", "cap_profile_id": "12345"},
            ],
        },
    ]
}


def test_harvest(tmp_path, test_session, mock_authors, caplog, requests_mock):
    caplog.set_level(logging.INFO)

    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # make sure the jsonl file looks good
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 3

    # make sure there are publications in the database
    with test_session.begin() as session:
        assert session.query(Publication).count() == 2, "two publications loaded"

        # Fetch all publications in a deterministic order
        pubs = session.query(Publication).order_by(Publication.id).all()

        pub = pubs[0]
        assert pub.doi == "10.1515/9781503624153", "first DOI present"
        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].cap_profile_id == "12345"
        assert pub.authors[1].cap_profile_id == "123456"

        pub2 = pubs[1]
        assert pub2.doi == "10.9999/0161754x-9809305", "second DOI present"
        assert len(pub2.authors) == 1, "publication has one author"
        assert pub2.authors[0].cap_profile_id == "12345"
        assert (
            "doi was not available in top level for sulpub id 456012 but found in identifier block"
            in caplog.text
        )


def test_harvest_when_doi_exists(
    tmp_path, test_session, mock_publication, mock_authors, requests_mock
):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 3

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 2, "two publications loaded"
        pub = session.query(Publication).order_by(Publication.id).first()

        assert pub.sulpub_json
        assert (
            pub.sulpub_json["title"] == "An example title with DOI in top level only"
        ), "sulpub json updated"
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
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 3

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 2, "two publications loaded"
        pub = session.query(Publication).order_by(Publication.id).first()

        assert pub.sulpub_json
        assert (
            pub.sulpub_json["title"] == "An example title with DOI in top level only"
        ), "sulpub json updated"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].cap_profile_id == "12345"
        assert pub.authors[1].cap_profile_id == "123456"
