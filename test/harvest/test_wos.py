import logging
import os
import re

import dotenv
import pandas
import pytest
import requests

from rialto_airflow.schema.harvest import Publication
from rialto_airflow.harvest import wos
from rialto_airflow.snapshot import Snapshot
from test.utils import num_jsonl_objects, num_log_record_matches

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


@pytest.fixture
def mock_no_wos_publication(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624199",
            sulpub_json={"sulpub": "data"},
            dim_json={"dim": "data"},
        )
        session.add(pub)
        return pub


@pytest.fixture
def mock_wos_doi(monkeypatch):
    """
    Mock our function for fetching publications by DOI from Web of Science.
    """

    def f(*args, **kwargs):
        yield {
            "doi": "10.1515/9781503624199",
            "title": "An example title",
            "type": "article",
            "publication_year": 1891,
        }

    monkeypatch.setattr(wos, "publications_from_dois", f)


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
    snapshot = Snapshot.create(tmp_path, "rialto_test")
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
    snapshot = Snapshot.create(tmp_path, "rialto_test")
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
    snapshot = Snapshot.create(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)
    assert "Reached limit of 50 publications stopping" in caplog.text


def test_customization_error(
    test_session, tmp_path, caplog, mock_authors, requests_mock
):
    """
    A 500 error (with a specific JSON error payload) we've seen from WoS.
    Now handled like any other 500 error, but confirm that we get the error
    message on the same log line as the HTTP error code.
    """
    requests_mock.get(
        re.compile(".*"),
        json={"message": "Customization error"},
        status_code=500,
        headers={"Content-Type": "application/json"},
    )

    snapshot = Snapshot.create(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)
    assert test_session().query(Publication).count() == 0, "no publications loaded"
    assert re.search("500 Server Error.*Customization error", caplog.text)


def test_not_found_error(test_session, tmp_path, caplog, mock_authors, requests_mock):
    """
    A 404 error from WoS should be logged, but should not stop harvesting.
    """
    requests_mock.get(
        re.compile(".*"),
        text="Not Found",
        reason="Not Found",
        status_code=404,
        headers={"Content-Type": "application/text"},
    )

    snapshot = Snapshot.create(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)
    assert test_session().query(Publication).count() == 0, "no publications loaded"
    assert (
        "404 Client Error: Not Found for url: https://wos-api.clarivate.com/api/wos?databaseId=WOK&usrQuery=AI"
        in caplog.text
    )


def test_server_error(test_session, tmp_path, caplog, mock_authors, requests_mock):
    """
    A 500 error from WoS should be logged, but should not stop harvesting.
    """
    requests_mock.get(
        re.compile(".*"),
        text="shrug",
        reason="Internal Server Error",
        status_code=500,
        headers={"Content-Type": "application/text"},
    )

    snapshot = Snapshot.create(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)
    assert test_session().query(Publication).count() == 0, "no publications loaded"
    assert (
        "500 Server Error: Internal Server Error for url: https://wos-api.clarivate.com/api/wos?databaseId=WOK&usrQuery=AI"
        in caplog.text
    )
    assert " -- shrug" in caplog.text


def test_empty_payload(test_session, tmp_path, caplog, mock_authors, requests_mock):
    """
    A 200 OK from WoS with an empty JSON payload should be skipped over.
    """
    requests_mock.get(re.compile(".*"), text="", status_code=200)

    snapshot = Snapshot.create(tmp_path, "rialto_test")
    wos.harvest(snapshot, limit=50)

    assert test_session().query(Publication).count() == 0, "no publications loaded"
    assert "got empty string instead of JSON" in caplog.text


def test_bad_wos_json(test_session, tmp_path, caplog, mock_authors, requests_mock):
    """
    A 200 OK from WoS with an empty JSON payload should be skipped over.
    """
    requests_mock.get(re.compile(".*"), text="ffff", status_code=200)

    snapshot = Snapshot.create(tmp_path, "rialto_test")

    with pytest.raises(requests.exceptions.JSONDecodeError):
        wos.harvest(snapshot, limit=50)

    assert "uhoh, instead of JSON we got: ffff" in caplog.text


def test_get_doi(caplog):
    wos_json_id_list = {
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {
                    "identifier": [
                        {"type": "issn", "value": "1234-5678"},
                        {"type": "doi", "value": "abc12310.1234/abc123"},
                    ]
                }
            }
        }
    }
    assert wos.get_doi(wos_json_id_list) == "10.1234/abc123"

    wos_json_single_id = {
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {
                    "identifier": {"type": "doi", "value": "abc12310.1234/abc123"}
                }
            }
        }
    }
    assert wos.get_doi(wos_json_single_id) == "10.1234/abc123"

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

    wos_json_identifiers_empty_string: dict = {
        "UID": "WOS:000089165000013",
        "dynamic_data": {"cluster_related": {"identifiers": ""}},
    }
    assert wos.get_doi(wos_json_identifiers_empty_string) is None
    assert "WOS:000089165000013" not in caplog.text

    wos_json_identifiers_nonempty_string: dict = {
        "UID": "WOS:000089165000014",
        "dynamic_data": {"cluster_related": {"identifiers": "abc123"}},
    }
    assert wos.get_doi(wos_json_identifiers_nonempty_string) == "abc123"
    assert "WOS:000089165000014" not in caplog.text

    wos_json_no_cluster_related_empty_string: dict = {
        "UID": "WOS:000012345000067",
        "dynamic_data": {"cluster_related": ""},
    }
    assert wos.get_doi(wos_json_no_cluster_related_empty_string) is None
    assert (
        "error 'str' object has no attribute 'get' trying to parse identifiers from {'UID': 'WOS:000012345000067'"
        in caplog.text
    )


def test_fill_in(snapshot, test_session, mock_no_wos_publication, mock_wos_doi, caplog):
    caplog.set_level(logging.INFO)
    wos.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.wos_json == {
            "doi": "10.1515/9781503624199",
            "title": "An example title",
            "type": "article",
            "publication_year": 1891,
        }

    # adds 1 publication to the jsonl file
    assert num_jsonl_objects(snapshot.path / "wos-fillin.jsonl") == 1
    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_wos(
    snapshot,
    test_session,
    mock_publication,
    mock_no_wos_publication,
    caplog,
    monkeypatch,
):
    caplog.set_level(logging.INFO)

    # make it look like wos returns no publications by DOI
    monkeypatch.setattr(wos, "publications_from_dois", lambda *args, **kwargs: [])
    wos.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624199")
            .first()
        )
        assert pub.wos_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "wos-fillin.jsonl") == 0
    assert "filled in 0 publications" in caplog.text


def test_publications_from_dois():
    # there are 231 DOIs in this list and publications_from_dois look them up in batches of 50
    dois = list(pandas.read_csv("test/data/dois.csv").doi)

    pubs = list(wos.publications_from_dois(dois))

    # the number of pubs we get back should exceed the batch size if the paging is working
    assert len(pubs) > 50


@pytest.fixture
def mock_wos_api(monkeypatch):
    def f(*args, **kwargs):
        query = args[0]
        if "my.unretrievable" in query:
            raise requests.exceptions.HTTPError(
                "https://wos-api.clarivate.com/api/wos?thisrequest=wontwork",
                400,
                "Server.Parser.NO_MATCHRULE, Translation Exception : NO_MATCHRULE:No matching rule found",
            )

        # NOTE: the below is not at all similar to the real API response, and
        # would have to be fleshed out if this mock was used for anything that
        # tries to use an API result.
        yield {"key": "value"}

    monkeypatch.setattr(wos, "_wos_api", f)


def test_publications_from_doi_batch_with_error_doi(mock_wos_api, caplog):
    dois = [
        "10.29309/tpmj/2015.22.05.1304",
        "10.1337/my.unretrievable.D01",  # should error in batch, and individually
        "10.1021/nl401130d",
        "10.1097/pcc.0000000000003026",
        "10.2331/suisan.19.1069",
        "10.1103/physrevb.53.r4221",
        "10.1130/b31546.1",
    ]

    pubs = list(wos.publications_from_dois(dois))

    assert len(pubs) == 6  # 6 of the 7 DOIs should return a result without error
    assert re.search(
        f"Unexpected error querying for DOIs in DOI batch, trying one at a time.*{dois[0]}.*{dois[-1]}.*error.*400: 'Server.Parser.NO_MATCHRULE, Translation Exception : NO_MATCHRULE:No matching rule found",
        caplog.text,
    )
    assert (
        'Unexpected error querying for single DOI from larger batch.  DOI="10.1337/my.unretrievable.D01"'
        in caplog.text
    )


@pytest.fixture
def mock_response_500(monkeypatch):
    """
    NOTE: The text property of the returned requests.Response object can be
    referenced without error; we just had trouble overriding it when first
    implementing this fixture, and overriding it wasn't essential to the tests
    first using the fixture.
    """

    def raise_for_status():
        raise requests.exceptions.HTTPError(
            "https://rest.service/api/path?param=value",
            500,
            "🤮",
        )

    mock_resp = requests.Response()
    monkeypatch.setattr(mock_resp, "raise_for_status", raise_for_status)
    return mock_resp


@pytest.fixture
def mock_response_200(monkeypatch):
    def raise_for_status():
        pass

    mock_resp = requests.Response()
    monkeypatch.setattr(mock_resp, "raise_for_status", raise_for_status)
    return mock_resp


def test_check_status_should_raise_true(mock_response_500, mock_response_200, caplog):
    with pytest.raises(requests.exceptions.HTTPError):
        wos.check_status(resp=mock_response_500, should_raise_for_status=True)

    assert (
        wos.check_status(resp=mock_response_200, should_raise_for_status=True) is True
    )

    assert num_log_record_matches(caplog.records, logging.ERROR, "🤮") == 1


def test_check_status_should_raise_false(mock_response_500, mock_response_200, caplog):
    assert (
        wos.check_status(resp=mock_response_500, should_raise_for_status=False) is False
    )
    assert (
        wos.check_status(resp=mock_response_200, should_raise_for_status=False) is True
    )

    assert num_log_record_matches(caplog.records, logging.ERROR, "🤮") == 1
