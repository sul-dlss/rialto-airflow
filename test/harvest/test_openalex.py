import dotenv
import json
import logging
import pytest

import pyalex
import requests
from rialto_airflow.harvest import openalex
from rialto_airflow.schema.harvest import Publication

from test.utils import num_jsonl_objects, num_log_record_matches

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


def test_harvest(snapshot, test_session, mock_authors, mock_openalex):
    # harvest from openalex
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
    snapshot, test_session, mock_publication, mock_authors, mock_openalex
):
    # harvest from openalex
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


def test_harvest_when_author_exists(
    snapshot,
    test_session,
    mock_publication,
    mock_authors,
    mock_association,
    mock_openalex,
):
    # harvest from openalex
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


def test_log_message(snapshot, mock_authors, mock_many_openalex, caplog):
    caplog.set_level(logging.INFO)
    openalex.harvest(snapshot, limit=50)
    assert "Reached limit of 50 publications stopping" in caplog.text


def mock_jsonl(path):
    """
    Mock the existing jsonl file for OpenAlex.
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


class MockWorks:
    def __init__(self, records):
        # create a
        self.records = records

    def filter(self, *args, **kwargs):
        # filter is a no-op when called
        return self

    def get(self):
        return self.records


def test_fill_in(snapshot, test_session, mock_publication, caplog, monkeypatch):
    caplog.set_level(logging.INFO)

    # setup Works to return a list of one record
    records = [
        {
            "doi": "10.1515/9781503624153",
            "title": "A sample title",
            "publication_year": 1891,
        }
    ]
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks(records))

    # set up a pre-existing jsonl file
    jsonl_file = snapshot.path / "openalex.jsonl"
    mock_jsonl(jsonl_file)

    openalex.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.openalex_json == {
            "doi": "10.1515/9781503624153",
            "title": "A sample title",
            "publication_year": 1891,
        }

    # adds 1 publication to the jsonl file
    assert num_jsonl_objects(snapshot.path / "openalex.jsonl") == 3
    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_openalex(
    test_session, mock_publication, snapshot, caplog, monkeypatch
):
    caplog.set_level(logging.INFO)

    # set up Works to return no records
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks([]))

    # set up a pre-existing jsonl file
    jsonl_file = snapshot.path / "openalex.jsonl"
    mock_jsonl(jsonl_file)

    openalex.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.openalex_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "openalex.jsonl") == 2
    assert "filled in 0 publications" in caplog.text


def test_fill_in_no_doi(test_session, mock_publication, snapshot, caplog, monkeypatch):
    """
    Test that Dimensions publication metadata lacking a DOI doesn't cause an
    exception during fill-in.
    """
    caplog.set_level(logging.INFO)

    # set up Works to return no records
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks([{"title": "example"}]))

    # set up a pre-existing jsonl file
    jsonl_file = snapshot.path / "openalex.jsonl"
    mock_jsonl(jsonl_file)

    openalex.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.openalex_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "openalex.jsonl") == 2
    assert "unable to determine what DOI to update" in caplog.text
    assert "filled in 0 publications" in caplog.text


def test_comma():
    """
    The OpenAlex API doesn't allow you to look up DOIs with commas in them. If
    this starts working again we can stop ignoring them when looking them up by
    DOI when doing the fill-in process.
    """
    with pytest.raises(requests.exceptions.HTTPError, match="Bad Request for url"):
        dois = "10.1103/physrevd.72,031101"
        pyalex.Works().filter(doi=dois).get()


def test_colon():
    """
    The OpenAlex API doesn't allow you to look up multiple DOIs if they contain
    a name prefix like 'doi:' since it confuses their query syntax into thinking you are trying to
    filter using an OR boolean. If this test starts passing we can consider
    stopping ignoring them.
    """
    with pytest.raises(requests.exceptions.HTTPError, match="Bad Request for url"):
        dois = "abc123|doi:abc123"
        pyalex.Works().filter(doi=dois).get()


def test_clean_dois_for_query(caplog):
    assert openalex._clean_dois_for_query(
        [
            "doi:123",
            "abc/123,45",
            "aaa/111",
            "123/abc pmcid:123",
            "abc/123",
            "10.1093/noajnl/vdad070.013pmcid:pmc10402389",
        ]
    ) == ["aaa/111", "abc/123"]

    assert num_log_record_matches(
        caplog.records,
        logging.WARNING,
        "dropped 4 DOIs from openalex lookup: ['doi:123', 'abc/123,45', '123/abc pmcid:123', '10.1093/noajnl/vdad070.013pmcid:pmc10402389']",
    )


class MockSources:
    def __init__(self, records):
        self.records = records

    def filter(self, *args, **kwargs):
        # filter is a no-op when called
        return self

    def get(self):
        return self.records


def test_source_by_issn(monkeypatch):
    records = [
        {
            "id": "https://openalex.org/S137773608",
            "issn_l": "0028-0836",
            "issn": ["0028-0836", "1476-4687"],
            "display_name": "Nature",
            "host_organization": "https://openalex.org/P4310319908",
            "host_organization_name": "Nature Portfolio",
            "works_count": 431710,
            "cited_by_count": 25659865,
        }
    ]
    monkeypatch.setattr(openalex, "Sources", lambda: MockSources(records))

    source = openalex.source_by_issn("0028-0836")
    assert source is not None
    assert source.get("display_name") == "Nature"
    assert source.get("host_organization_name") == "Nature Portfolio"


class MockSourcesError:
    def __init__(self, records):
        self.records = records

    def filter(self, *args, **kwargs):
        # filter is a no-op when called
        return self

    def get(self):
        raise requests.exceptions.JSONDecodeError("Expecting value", "", 0)


def test_source_by_issn_jsonerror(monkeypatch, caplog):
    records = "Not JSON"
    monkeypatch.setattr(openalex, "Sources", lambda: MockSourcesError(records))

    source = openalex.source_by_issn("XXXX-0836")
    assert source is None
    assert "Error decoding JSON for ISSN XXXX-0836" in caplog.text
