import json
import logging
import pytest
import re
import dotenv

from rialto_airflow.schema.harvest import Publication
from rialto_airflow.harvest import pubmed
from test.utils import num_jsonl_objects, load_jsonl_file

dotenv.load_dotenv()


@pytest.fixture
def mock_pubmed_fetch(monkeypatch):
    """
    Mock our function for fetching publications from a list of PMIDs from Pubmed.
    """

    def f(*args, **kwargs):
        return load_jsonl_file("test/data/pubmed.jsonl")

    monkeypatch.setattr(pubmed, "publications_from_pmids", f)


@pytest.fixture
def mock_pubmed_search(monkeypatch):
    """
    Mock our function for searching for PMIDs given an ORCID
    """

    def f(*args, **kwargs):
        return ["36857419", "36108252"]

    monkeypatch.setattr(pubmed, "pmids_from_orcid", f)


@pytest.fixture
def existing_publication(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1182/bloodadvances.2022008893",
            sulpub_json={"sulpub": "data"},
        )
        session.add(pub)
        return pub


def mock_jsonl(path):
    """
    Mock the existing jsonl file for Pubmed
    """
    records = [
        {
            "MedlineCitation": {
                "Article": {
                    "ArticleTitle": "Example Title",
                },
            },
            "PubmedData": {
                "ArticleIdList": {
                    "ArticleId": [
                        {"@IdType": "pubmed", "#text": "36857419"},
                        {"@IdType": "doi", "#text": "10.1182/bloodadvances.2022008893"},
                    ]
                },
            },
        },
        {
            "MedlineCitation": {
                "Article": {
                    "ArticleTitle": "Another Article Title",
                },
            },
            "PubmedData": {
                "ArticleIdList": {
                    "ArticleId": [
                        {"@IdType": "pubmed", "#text": "21302935"},
                        {"@IdType": "doi", "#text": "10.1021/ac1028984"},
                    ]
                },
            },
        },
    ]
    with open(path, "w") as f:
        for record in records:
            f.write(f"{json.dumps(record)}\n")


def jsonl_file(path):
    return path / "pubmed.jsonl"


def pubmed_json():
    """
    A partial Pubmed JSON object with a DOI and some other IDs.
    """
    return {
        "MedlineCitation": {
            "Article": {
                "ELocationID": [
                    {
                        "@EIdType": "doi",
                        "@ValidYN": "Y",
                        "#text": "10.1021/ac1028984",
                    }
                ],
                "ArticleTitle": "Another Article Title",
            },
        },
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": [
                    {"@IdType": "pubmed", "#text": "36857419"},
                    {"@IdType": "pmc", "#text": "PMC10275701"},
                    {"@IdType": "doi", "#text": "10.1182/bloodadvances.2022008893"},
                    {"@IdType": "pii", "#text": "494746"},
                ]
            },
        },
    }


def pubmed_json_no_doi():
    """
    A partial Pubmed JSON object without a DOI.
    """
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Another Article Title",
            },
        },
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": [
                    {"@IdType": "pubmed", "#text": "36857419"},
                    {"@IdType": "pmc", "#text": "PMC10275701"},
                    {"@IdType": "pii", "#text": "494746"},
                ]
            },
        },
    }


def pubmed_json_fill_in_doi():
    """
    A partial Pubmed JSON object with a DOI.
    """
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Another Article Title To Be Filled In",
            },
        },
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": [
                    {"@IdType": "pubmed", "#text": "12345"},
                    {"@IdType": "doi", "#text": "10.1515/9781503624153"},
                ]
            },
        },
    }


def test_pubmed_search_no_results(requests_mock, caplog):
    """
    This is a test of the Pubmed Search API to ensure we are parsing the mocked response correctly with no results.
    """
    caplog.set_level(logging.INFO)

    requests_mock.get(
        re.compile(".*"),
        json={
            "header": {"type": "esearch", "version": "0.3"},
            "esearchresult": {"count": "0"},
        },
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    assert pubmed.pmids_from_orcid("nope") == []


def test_pubmed_search_error(requests_mock, caplog):
    """
    This is a test of the Pubmed Search API to ensure we are parsing the mocked response correctly when an error.
    """
    caplog.set_level(logging.INFO)

    requests_mock.get(
        re.compile(".*"),
        json={"error": "no good"},
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    assert pubmed.pmids_from_orcid("nope") == []
    assert "Error in results found for nope[auid]: no good" in caplog.text


def test_pubmed_search_unexpected_response(requests_mock, caplog):
    """
    This is a test of the Pubmed Search API to ensure we are parsing the mocked response correctly when there is an unexpected response.
    """
    caplog.set_level(logging.INFO)

    requests_mock.get(
        re.compile(".*"),
        json={"header": {"type": "esearch", "version": "0.3"}},
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    assert pubmed.pmids_from_orcid("nope") == []
    assert "No esearchresult or count found for nope" in caplog.text


def test_pubmed_search_handles_500(requests_mock, caplog):
    """
    This is a test of the Pubmed Search API to ensures we don't crash with a 500 response.
    """
    caplog.set_level(logging.INFO)

    requests_mock.get(
        re.compile(".*"),
        json={},
        status_code=500,
        headers={"Content-Type": "application/json"},
    )
    result = pubmed.pmids_from_orcid("12345")
    assert result == []
    assert (
        "Error searching pubmed query {'term': '12345[auid]'}: 500 Server Error"
        in caplog.text
    )


def test_pubmed_search_orcid_found_publications():
    """
    This is a live test of the Pubmed Search API to ensure we can get PMIDs back given an ORCID.
    """
    # The ORCID that is tested should return more at least two PMIDs
    orcid = "https://orcid.org/0000-0002-5286-7795"
    pmids = pubmed.pmids_from_orcid(orcid)
    assert len(pmids) >= 2, "found at least 2 publications"
    assert "29035265" in pmids, "found an expected publication for this author"


def test_pubmed_search_orcid_no_publications():
    """
    This is a live test of the Pubmed Search API to ensure no results are returned for an ORCID with no publications.
    """
    # No publications should be found for this ORCID
    orcid = "5555-5555-5555-5555"
    pmids = pubmed.pmids_from_orcid(orcid)
    assert len(pmids) == 0, "found no publications"


def test_pubmed_search_dois_found_publications():
    """
    This is a live test of the Pubmed Search API to ensure we can get PMIDs back given two DOIs.
    """
    # The first DOIs should both return PMIDs, the last will return nothing
    dois = ["10.1118/1.598623", "10.3899/jrheum.220960", "bogus"]
    pmids = pubmed.pmids_from_dois(dois)
    assert set(pmids) == {"10435530", "36243410"}, (
        "found both publications"
    )  # ordering is not important


def test_pubmed_search_dois_found_one_publication():
    """
    This is a live test of the Pubmed Search API to ensure we can get PMIDs back a single DOIs.
    """
    # These DOIs should both return PMIDs
    dois = ["10.1021/ac1028984"]
    pmids = pubmed.pmids_from_dois(dois)
    assert pmids == ["21302935"], "found single publication"


def test_pubmed_search_dois_no_publications():
    """
    This is a live test of the Pubmed Search API to ensure no results are returned for DOIs with no publications.
    """
    # No publications should be found for these dois
    dois = ["bogus-doi-1", "bogus-doi-2"]
    pmids = pubmed.pmids_from_dois(dois)
    assert len(pmids) == 0, "found no publications"


def test_pubmed_fetch_publications():
    """
    This is a live test of the Pubmed Fetch API to ensure we can get publication data back given a list of PMIDs.
    """
    # Both of these publications should be found
    pmids = ["29035265", "29035260"]
    pubs = pubmed.publications_from_pmids(pmids)
    assert len(pubs) == 2, "found both publications"
    assert isinstance(pubs[0], dict), (
        "first publication is json"
    )  # check that we got a dict back
    assert isinstance(pubs[1], dict), (
        "second publication is json"
    )  # check that we got a dict back
    assert "PubmedData" in pubs[0], "found the PubmedData key in the first publication"
    assert "PubmedData" in pubs[1], "found the PubmedData key in the second publication"


def test_pubmed_fetch_missing_publications():
    """
    This is a live test of the Pubmed Fetch API to ensure we can get a list back even for one publication
    """
    # This publication should be found
    pmids = ["29035265"]
    pubs = pubmed.publications_from_pmids(pmids)

    assert len(pubs) == 1, "found publication as a list of one element"
    assert isinstance(pubs[0], dict), (
        "first publication is json"
    )  # check that we got a dict back
    assert "PubmedData" in pubs[0], "found the PubmedData key in the first publication"


def test_pubmed_fetch_handles_500(requests_mock, caplog):
    """
    This is a test of the Pubmed Fetch API to ensures we don't crash with a 500 response.
    """
    caplog.set_level(logging.INFO)

    requests_mock.post(
        re.compile(".*"),
        json={},
        status_code=500,
        headers={"Content-Type": "application/json"},
    )
    result = pubmed.publications_from_pmids(["12345"])
    assert result == []
    assert (
        "Error fetching full pubmed records id=12345: 500 Server Error" in caplog.text
    )


def test_pubmed_fetch_publications_expects_list():
    pubs = pubmed.publications_from_pmids([])

    assert pubs == [], "no publications returned because we passed an empty list"


def test_harvest(
    snapshot, test_session, mock_authors, mock_pubmed_fetch, mock_pubmed_search
):
    """
    With some authors loaded and a mocked Pubmed API, make sure that
    publications are matched up to the authors using the ORCID.
    """
    # harvest from Pubmed
    pubmed.harvest(snapshot)

    # the mocked Pubmed api returns the same two publications for both authors
    assert num_jsonl_objects(jsonl_file(snapshot.path)) == 4

    # make sure a publication is in the database and linked to the author
    with test_session.begin() as session:
        assert session.query(Publication).count() == 2, "two publications loaded"

        pubs = session.query(Publication).all()
        assert pubs[0].doi == "10.1182/bloodadvances.2022008893", "doi was added"
        assert pubs[1].doi == "10.1200/jco.22.01076", "doi was normalized"

        assert len(pubs[0].authors) == 2, "publication has two authors"
        assert pubs[0].authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pubs[0].authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_doi_exists(
    snapshot,
    test_session,
    existing_publication,
    mock_authors,
    mock_pubmed_fetch,
    mock_pubmed_search,
):
    """
    When a publication and its authors already exist in the database make sure that the pubmed_json is updated.
    """
    # harvest from Pubmed
    pubmed.harvest(snapshot)

    # jsonl file is there and has four lines (two pubs for each author)
    assert num_jsonl_objects(jsonl_file(snapshot.path)) == 4

    # ensure that the existing publication for the DOI was updated
    with test_session.begin() as session:
        assert session.query(Publication).count() == 2, "two publications loaded"
        pub = session.query(Publication).first()

        assert pub.wos_json is None
        assert pub.sulpub_json == {"sulpub": "data"}, "sulpub data the same"
        assert pub.pubmed_json

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_fill_in(snapshot, test_session, mock_publication, caplog, monkeypatch):
    caplog.set_level(logging.INFO)

    # mock pubmed api to return a PMID for the fake DOI
    monkeypatch.setattr(pubmed, "pmids_from_dois", lambda *args, **kwargs: ["12345"])

    # mock pubmed api to return a record for this PMID
    monkeypatch.setattr(
        pubmed,
        "publications_from_pmids",
        lambda *args, **kwargs: [pubmed_json_fill_in_doi()],
    )

    # set up a pre-existing jsonl file
    mock_jsonl(jsonl_file(snapshot.path))

    pubmed.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.pubmed_json == pubmed_json_fill_in_doi()

    # adds 1 publication to the jsonl file
    assert num_jsonl_objects(jsonl_file(snapshot.path)) == 3
    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_pubmed(
    test_session, mock_publication, snapshot, caplog, monkeypatch
):
    caplog.set_level(logging.INFO)

    # mock pubmed api to return no records for the doi
    monkeypatch.setattr(pubmed, "pmids_from_dois", lambda *args, **kwargs: [])

    # set up a pre-existing jsonl file
    mock_jsonl(jsonl_file(snapshot.path))

    pubmed.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.pubmed_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(jsonl_file(snapshot.path)) == 2
    assert "filled in 0 publications" in caplog.text


def test_fill_in_no_doi(test_session, mock_publication, snapshot, caplog, monkeypatch):
    """
    Test that a publication coming back from Pubmed without DOI doesn't
    cause an exception.
    """

    # mock pubmed api to return a PMID for the fake DOI
    monkeypatch.setattr(pubmed, "pmids_from_dois", lambda *args, **kwargs: ["12345"])

    # mock pubmed api to return a record for this PMID, but without a DOI
    monkeypatch.setattr(
        pubmed,
        "publications_from_pmids",
        lambda *args, **kwargs: [pubmed_json_no_doi()],
    )

    caplog.set_level(logging.INFO)

    # set up a pre-existing jsonl file
    mock_jsonl(jsonl_file(snapshot.path))
    assert num_jsonl_objects(jsonl_file(snapshot.path)) == 2

    pubmed.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.pubmed_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(jsonl_file(snapshot.path)) == 2
    assert "unable to determine what DOI to update" in caplog.text
    assert "filled in 0 publications" in caplog.text


def test_get_doi():
    assert pubmed.get_doi(pubmed_json()) == "10.1182/bloodadvances.2022008893"

    pubmed_json_single_id = {
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": {
                    "@IdType": "doi",
                    "#text": "10.1182/bloodadvances.2022008893",
                },
            },
        }
    }
    assert pubmed.get_doi(pubmed_json_single_id) == "10.1182/bloodadvances.2022008893"

    assert pubmed.get_doi(pubmed_json_no_doi()) is None

    pubmed_json_no_ids = {
        "PubmedData": {
            "PublicationStatus": "ppublish",
        }
    }
    assert pubmed.get_doi(pubmed_json_no_ids) is None

    pubmed_json_alt_doi = {
        "MedlineCitation": {
            "Article": {
                "ELocationID": [
                    {
                        "@EIdType": "doi",
                        "@ValidYN": "Y",
                        "#text": "10.1182/bloodadvances.2022008893",
                    }
                ]
            },
        }
    }
    assert pubmed.get_doi(pubmed_json_alt_doi) == "10.1182/bloodadvances.2022008893"

    pubmed_json_alt_single_id = {
        "MedlineCitation": {
            "Article": {
                "ELocationID": {
                    "@EIdType": "doi",
                    "@ValidYN": "Y",
                    "#text": "10.1182/only-one",
                }
            },
        }
    }
    assert pubmed.get_doi(pubmed_json_alt_single_id) == "10.1182/only-one"


def test_get_identifier():
    assert (
        pubmed.get_identifier(pubmed_json(), "doi")
        == "10.1182/bloodadvances.2022008893"
    )
    assert pubmed.get_identifier(pubmed_json(), "pubmed") == "36857419"
    assert pubmed.get_identifier(pubmed_json(), "pmc") == "PMC10275701"
    assert pubmed.get_identifier(pubmed_json(), "pii") == "494746"
    assert pubmed.get_identifier(pubmed_json(), "invalid") is None
