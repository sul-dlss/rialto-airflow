import os
import logging

import dotenv
import pytest

from rialto_airflow.schema.rialto import Publication
from rialto_airflow.harvest_incremental import sul_pub
from rialto_airflow.snapshot import Snapshot

from test.utils import num_jsonl_objects, num_log_record_matches


@pytest.fixture
def mock_rialto_db_name(monkeypatch):
    monkeypatch.setattr(sul_pub, "RIALTO_DB_NAME", "rialto_incremental_test")


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


def test_harvest(
    tmp_path,
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    caplog,
    requests_mock,
):
    caplog.set_level(logging.DEBUG)

    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_incremental_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # make sure the jsonl file looks good
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 3

    # make sure there are publications in the database
    with test_incremental_session.begin() as session:
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
            num_log_record_matches(
                caplog.records,
                logging.DEBUG,
                "doi was not available in top level for sulpub id 456012 but found in identifier block",
            )
            == 1
        )


def test_harvest_limit(
    tmp_path,
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    caplog,
    requests_mock,
):
    """
    Confirm that the harvest limit we use in test envs is observed, and that a warning is
    logged when the limit is hit.
    """
    caplog.set_level(logging.DEBUG)

    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub with a limit of one publication
    snapshot = Snapshot.create(tmp_path, "rialto_incremental_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key, limit=1)

    # make sure the jsonl file looks good
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 1

    # make sure a publication is in the database and linked to the authors
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publications loaded"

        pubs = session.query(Publication).all()
        assert pubs[0].doi == "10.1515/9781503624153", "doi was added"

        assert len(pubs[0].authors) == 2, (
            "publication has two authors because limit is only placed on publication processing"
        )
        assert pubs[0].authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"

        assert (
            num_log_record_matches(
                caplog.records,
                logging.WARNING,
                "stopping with limit=1",
            )
            == 1
        )


def test_harvest_when_doi_exists(
    tmp_path,
    test_incremental_session,
    mock_incremental_publication,
    mock_incremental_authors,
    mock_rialto_db_name,
    requests_mock,
):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_incremental_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 3

    # ensure that the existing publication for the DOI was updated
    with test_incremental_session.begin() as session:
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
    test_incremental_session,
    mock_incremental_publication,
    mock_incremental_authors,
    mock_incremental_association,
    mock_rialto_db_name,
    requests_mock,
):
    requests_mock.get("/publications.json", json=response)
    requests_mock.get("/publications.json?page=2", json={"records": []})

    # harvest from sulpub
    snapshot = Snapshot.create(tmp_path, "rialto_incremental_test")
    sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key)

    # jsonl file is there and ok
    assert num_jsonl_objects(snapshot.path / "sulpub.jsonl") == 3

    # ensure that the existing publication for the DOI was updated
    with test_incremental_session.begin() as session:
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


def test_extract_wos_uid_from_top_level():
    pub = {"wos_uid": "WOS:001008232900698"}
    assert sul_pub.extract_wos_uid(pub) == "001008232900698"


def test_extract_wos_uid_from_identifier_wos_uid_type():
    pub = {"identifier": [{"type": "WosUID", "id": "WOS:001008232900698"}]}
    assert sul_pub.extract_wos_uid(pub) == "001008232900698"


def test_extract_wos_uid_from_identifier_wos_item_id_type():
    pub = {"identifier": [{"type": "WoSItemID", "id": "001008232900698"}]}
    assert sul_pub.extract_wos_uid(pub) == "001008232900698"


def test_extract_wos_uid_from_identifier_alternate_wos_item_id_type():
    pub = {"identifier": [{"type": "WosItemID", "id": "001008232900698"}]}
    assert sul_pub.extract_wos_uid(pub) == "001008232900698"


def test_extract_wos_uid_returns_none_when_absent():
    pub = {"identifier": [{"type": "doi", "id": "10.1000/xyz123"}]}
    assert sul_pub.extract_wos_uid(pub) is None


def test_extract_pmid_from_top_level():
    pub = {"pmid": 29780978}
    assert sul_pub.extract_pmid(pub) == "29780978"


def test_extract_pmid_from_identifier():
    pub = {"identifier": [{"type": "pmid", "id": "29780978"}]}
    assert sul_pub.extract_pmid(pub) == "29780978"


def test_extract_pmid_from_identifier_uppercase_type():
    pub = {"identifier": [{"type": "PMID", "id": "29780978"}]}
    assert sul_pub.extract_pmid(pub) == "29780978"


def test_extract_pmid_returns_none_when_absent():
    pub = {"identifier": [{"type": "doi", "id": "10.1000/xyz123"}]}
    assert sul_pub.extract_pmid(pub) is None
