import logging
import os
import re
from dataclasses import dataclass

import dotenv
import pytest
import requests

from rialto_airflow.database import Publication
from rialto_airflow.harvest import wos
from rialto_airflow.snapshot import Snapshot
from test.utils import num_jsonl_objects

dotenv.load_dotenv()


wos_key = os.environ.get("AIRFLOW_VAR_WOS_KEY")


@pytest.fixture
def mock_wos(monkeypatch):
    """
    Mock our function for fetching publications by orcid from Web of Science.
    """

    def f(*args, **kwargs):
        yield {
            "static_data": {
                "summary": {
                    "titles": {
                        "title": [{"type": "source", "content": "An example title"}]
                    }
                }
            },
            "dynamic_data": {
                "cluster_related": {
                    "identifiers": {
                        "identifier": [
                            {"type": "issn", "value": "2211-9124"},
                            {
                                "type": "doi",
                                "value": "https://doi.org/10.1515/9781503624153",
                            },
                        ]
                    }
                }
            },
        }

    monkeypatch.setattr(wos, "orcid_publications", f)


@pytest.fixture
def mock_many_wos(monkeypatch):
    """
    A fixture for that returns 1000 fake documents from Web of Science.
    """

    def f(*args, **kwargs):
        for n in range(1, 1000):
            yield {"UID": f"mock:{n}"}

    monkeypatch.setattr(wos, "orcid_publications", f)


@pytest.fixture
def existing_publication(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            sulpub_json={"sulpub": "data"},
        )
        session.add(pub)
        return pub


@pytest.mark.skipif(wos_key is None, reason="no Web of Science key")
def test_orcid_publications_with_paging():
    """
    This is a live test of WoS API to ensure paging works properly.
    """

    # https://www-webofscience-com.stanford.idm.oclc.org/wos/alldb/advanced-search
    # The ORCID that is tested should return more than 200 results to exercise paging
    orcid = "https://orcid.org/0000-0002-0673-5257"

    uids = set()
    for pub in wos.orcid_publications(orcid):
        assert pub
        assert pub["UID"] not in uids, "haven't seen publication before"
        uids.add(pub["UID"])

    assert len(uids) > 200, "found more than 200 publications"


@pytest.mark.skipif(wos_key is None, reason="no Web of Science key")
def test_orcid_publications_with_bad_orcid():
    """
    This is a live test of the WoS API to ensure that a search for an invalid ORCID yields no results.
    """
    assert (
        len(list(wos.orcid_publications("https://orcid.org/0000-0003-0784-7987-XXX")))
        == 0
    )


def test_harvest(tmp_path, test_session, mock_authors, mock_wos):
    """
    With some authors loaded and a mocked WoS API make sure that a
    publication is matched up to the authors using the ORCID.
    """
    # harvest from Web of Science
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    wos.harvest(snapshot)

    # the mocked Web of Science api returns the same publication for both authors
    assert num_jsonl_objects(snapshot.path / "wos.jsonl") == 2

    # make sure a publication is in the database and linked to the author
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert pub.doi == "10.1515/9781503624153", "doi was normalized"

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_doi_exists(
    tmp_path, test_session, existing_publication, mock_authors, mock_wos
):
    """
    When a publication and its authors already exist in the database make sure that the wos_json is updated.
    """
    # harvest from web of science
    snapshot = Snapshot(path=tmp_path, database_name="rialto_test")
    wos.harvest(snapshot)

    # jsonl file is there and has two lines (one for each author)
    assert num_jsonl_objects(snapshot.path / "wos.jsonl") == 2

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.wos_json
        assert pub.sulpub_json == {"sulpub": "data"}, "sulpub data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_log_message(tmp_path, mock_authors, mock_many_wos, caplog):
    caplog.set_level(logging.INFO)
    snapshot = Snapshot(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)
    assert "Reached limit of 50 publications stopping" in caplog.text


@dataclass
class MockResponse:
    status_code: int = 500
    content: str = ""


def test_customization_error(
    test_session, tmp_path, caplog, mock_authors, requests_mock
):
    """
    A 500 error from WoS with a specific JSON error payload should be skipped over.
    """
    requests_mock.get(
        re.compile(".*"),
        json={"message": "Customization error"},
        status_code=500,
        headers={"Content-Type": "application/json"},
    )

    snapshot = Snapshot(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)
    assert test_session().query(Publication).count() == 0, "no publications loaded"
    assert "got a 500 Customization Error" in caplog.text


def test_empty_payload(test_session, tmp_path, caplog, mock_authors, requests_mock):
    """
    A 200 OK from WoS with an empty JSON payload should be skipped over.
    """
    requests_mock.get(re.compile(".*"), text="", status_code=200)

    snapshot = Snapshot(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)

    assert test_session().query(Publication).count() == 0, "no publications loaded"
    assert "got empty string instead of JSON" in caplog.text


def test_bad_wos_json(test_session, tmp_path, caplog, mock_authors, requests_mock):
    """
    A 200 OK from WoS with an empty JSON payload should be skipped over.
    """
    requests_mock.get(re.compile(".*"), text="ffff", status_code=200)

    snapshot = Snapshot(tmp_path, "rialto_test")

    with pytest.raises(requests.exceptions.JSONDecodeError):
        wos.harvest(snapshot, limit=50)

    assert "uhoh, instead of JSON we got: ffff" in caplog.text


def test_get_doi():
    wos_json_id_list = {
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {
                    "identifier": [
                        {"type": "issn", "value": "1234-5678"},
                        {"type": "doi", "value": "abc123"},
                    ]
                }
            }
        }
    }
    assert wos.get_doi(wos_json_id_list) == "abc123"

    wos_json_single_id = {
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {"identifier": {"type": "doi", "value": "abc123"}}
            }
        }
    }
    assert wos.get_doi(wos_json_single_id) == "abc123"

    wos_json_no_doi = {
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {"identifier": {"type": "issn", "value": "1234-5678"}}
            }
        }
    }
    assert wos.get_doi(wos_json_no_doi) is None

    wos_json_no_id: dict = {"dynamic_data": {"cluster_related": {"identifiers": {}}}}
    assert wos.get_doi(wos_json_no_id) is None
