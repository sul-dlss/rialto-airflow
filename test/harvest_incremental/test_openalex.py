import datetime
import logging
import pytest

import pyalex
import requests

from rialto_airflow.harvest_incremental import openalex
from rialto_airflow.schema.rialto import Publication, Harvest, Author

from test.utils import num_log_record_matches


def test_publications_from_orcid():
    """
    This is a live test of OpenAlex API, to make sure paging works properly.
    """
    count = 0
    for pub in openalex.publications_from_orcid(
        "https://orcid.org/0000-0002-8030-5327", harvest_date=None
    ):
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
            "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/36857419"},
        }

    monkeypatch.setattr(openalex, "publications_from_orcid", f)


def test_harvest(
    test_incremental_session,
    mock_incremental_authors,
    mock_openalex,
    mock_rialto_db_name,
):
    # harvest from openalex
    openalex.harvest()

    # make sure a publication is in the database and linked to the author
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"

        pub = session.query(Publication).first()
        assert pub.doi == "10.1515/9781503624153", "doi was normalized"
        assert pub.pubmed_id == "36857419", "pubmed_id populated from ids.pmid"

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_doi_exists(
    test_incremental_session,
    mock_incremental_publication,
    mock_incremental_authors,
    mock_openalex,
    mock_rialto_db_name,
):
    # harvest from openalex
    openalex.harvest()

    # ensure that the existing publication for the DOI was updated
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.openalex_json
        assert pub.openalex_json["title"] == "An example title", "openalex json updated"
        assert pub.pubmed_id == "36857419", "pubmed_id populated from ids.pmid"
        assert pub.wos_json == {"wos": "data"}, "wos data the same"
        assert pub.pubmed_json is None

        assert len(pub.authors) == 2, "publication has two authors"
        assert pub.authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pub.authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_when_author_exists(
    test_incremental_session,
    mock_incremental_publication,
    mock_incremental_authors,
    mock_incremental_association,
    mock_openalex,
    mock_rialto_db_name,
):
    # harvest from openalex
    openalex.harvest()

    # ensure that the existing publication for the DOI was updated
    with test_incremental_session.begin() as session:
        assert session.query(Publication).count() == 1, "one publication loaded"
        pub = session.query(Publication).first()

        assert pub.openalex_json
        assert pub.openalex_json["title"] == "An example title", "openalex json updated"
        assert pub.pubmed_id == "36857419", "pubmed_id populated from ids.pmid"
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

    monkeypatch.setattr(openalex, "publications_from_orcid", f)


def test_log_message(
    mock_incremental_authors,
    mock_many_openalex,
    mock_rialto_db_name,
    caplog,
):
    caplog.set_level(logging.INFO)
    openalex.harvest(limit=50)
    assert "Reached limit of 50 publications or authors, stopping" in caplog.text


class MockWorks:
    def __init__(self, records):
        # create a
        self.records = records

    def filter(self, *args, **kwargs):
        # filter is a no-op when called
        return self

    def get(self):
        return self.records


def test_fill_in(
    test_incremental_session,
    mock_incremental_publication,
    mock_rialto_db_name,
    caplog,
    active_harvest_id,
    monkeypatch,
):
    caplog.set_level(logging.INFO)

    # setup Works to return a list of one record
    records = [
        {
            "doi": "10.1515/9781503624153",
            "title": "A sample title",
            "publication_year": 1891,
            "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/36857419"},
        }
    ]
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks(records))
    openalex.fill_in(active_harvest_id)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.openalex_json == {
            "doi": "10.1515/9781503624153",
            "title": "A sample title",
            "publication_year": 1891,
            "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/36857419"},
        }
        assert pub.pubmed_id == "36857419", "pubmed_id populated from ids.pmid"

    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_openalex(
    test_incremental_session,
    mock_incremental_publication,
    mock_rialto_db_name,
    caplog,
    active_harvest_id,
    monkeypatch,
):
    caplog.set_level(logging.INFO)

    # set up Works to return no records
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks([]))
    openalex.fill_in(active_harvest_id)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.openalex_json is None

    assert "filled in 0 publications" in caplog.text


def test_fill_in_no_doi(
    test_incremental_session,
    mock_incremental_publication,
    mock_rialto_db_name,
    active_harvest_id,
    caplog,
    monkeypatch,
):
    """
    Test that Dimensions publication metadata lacking a DOI doesn't cause an
    exception during fill-in.
    """
    caplog.set_level(logging.INFO)

    # set up Works to return no records
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks([{"title": "example"}]))
    openalex.fill_in(active_harvest_id)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.openalex_json is None

    assert "unable to determine what DOI to update" in caplog.text
    assert "filled in 0 publications" in caplog.text


def test_fill_in_none_doi(
    test_incremental_session,
    mock_incremental_publication,
    mock_rialto_db_name,
    active_harvest_id,
    caplog,
    monkeypatch,
):
    """
    Test that publications with a None DOI are not used to do lookups.
    """
    caplog.set_level(logging.INFO)

    # set up Works to return no records
    monkeypatch.setattr(openalex, "Works", lambda: MockWorks([{"doi": None}]))
    openalex.fill_in(active_harvest_id)

    with test_incremental_session.begin() as session:
        pub = session.query(Publication).where(Publication.doi is None).first()
        assert pub is None

    assert "unable to determine what DOI to update" in caplog.text
    assert "filled in 0 publications" in caplog.text


def test_fill_in_filters_publications_using_harvest_created_at(
    test_incremental_session, mock_rialto_db_name, monkeypatch, active_harvest_id
):
    """
    Ensure that only publications that have been updated since the active
    harvest started will be filled in.
    """

    with test_incremental_session.begin() as session:
        session.add_all(
            [
                Publication(
                    doi="10.1111/older",
                    openalex_json=None,
                    updated_at=datetime.datetime(2025, 12, 31, 0, 0, 0),
                ),
                Publication(
                    doi="10.1111/newer",
                    openalex_json=None,
                    updated_at=datetime.datetime(2026, 4, 30, 0, 0, 0),
                ),
            ]
        )

    queried_dois = []

    def _capture_dois(doi):
        queried_dois.append(doi)
        return doi

    monkeypatch.setattr(openalex, "normalize_doi", _capture_dois)

    openalex.fill_in(harvest_id=active_harvest_id)

    assert queried_dois == ["10.1111/newer"], (
        "only publications updated after the selected harvest created_at should be queried"
    )


def test_comma():
    """
    The OpenAlex API doesn't allow you to look up DOIs with commas in them. If
    this starts working again we can stop ignoring them when looking them up by
    DOI when doing the fill-in process.
    """
    with pytest.raises(requests.exceptions.HTTPError, match="400 Client Error"):
        dois = "10.1103/physrevd.72,031101"
        pyalex.Works().filter(doi=dois).get()


def test_colon():
    """
    The OpenAlex API doesn't allow you to look up multiple DOIs if they contain
    a name prefix like 'doi:' since it confuses their query syntax into thinking you are trying to
    filter using an OR boolean. If this test starts passing we can consider
    stopping ignoring them.
    """
    with pytest.raises(
        pyalex.api.QueryError, match="It looks like you're trying to do an OR query"
    ):
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


def test_harvest_passes_previous_harvest_date_to_orcid_query(
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    monkeypatch,
):
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 4, 27, 16, 38, 10),
                finished_at=datetime.datetime(2026, 4, 28, 0, 0, 0),
            )
        )
        # Make sure Authors are similar to those loaded in production, with timestamps.
        session.query(Author).where(Author.orcid.is_not(None)).update(
            {
                Author.created_at: datetime.datetime(2026, 4, 20, 0, 0, 0),
                Author.updated_at: datetime.datetime(2026, 4, 20, 0, 0, 0),
            }
        )

    harvest_dates = []

    def _capture_harvest_date(orcid, harvest_date=None):
        # capture the harvest_date that is passed to publications_from_orcid so that we can assert on it later
        harvest_dates.append(harvest_date)
        yield from ()

    monkeypatch.setattr(openalex, "publications_from_orcid", _capture_harvest_date)

    openalex.harvest()
    assert harvest_dates, (
        "publications_from_orcid is passed the recent finished harvest date"
    )
    assert set(harvest_dates) == {"2026-04-27T16:38:10"}, (
        "formatted previous harvest date passed to publications_from_orcid"
    )


def test_harvest_omits_previous_harvest_date_for_recently_updated_authors(
    test_incremental_session,
    mock_incremental_authors,
    mock_rialto_db_name,
    monkeypatch,
):
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 4, 25, 16, 38, 10),
                finished_at=datetime.datetime(2026, 4, 28, 0, 0, 0),
            )
        )
        session.query(Author).where(
            Author.orcid == "https://orcid.org/0000-0000-0000-0001"
        ).update(
            {
                Author.created_at: datetime.datetime(2025, 1, 1, 0, 0, 0),
                Author.updated_at: datetime.datetime(2026, 4, 28, 0, 0, 0),
            }
        )
        session.query(Author).where(
            Author.orcid == "https://orcid.org/0000-0000-0000-0002"
        ).update(
            {
                Author.created_at: datetime.datetime(2025, 1, 20, 0, 0, 0),
                Author.updated_at: datetime.datetime(2025, 1, 1, 0, 0, 0),
            }
        )

    harvest_dates = []

    def _capture_harvest_date(orcid, harvest_date=None):
        harvest_dates.append(harvest_date)
        yield from ()

    monkeypatch.setattr(openalex, "publications_from_orcid", _capture_harvest_date)

    openalex.harvest()

    assert harvest_dates, (
        "publications_from_orcid should be called with harvest_date for ORCID authors"
    )
    assert len(harvest_dates) == 2, "two ORCID authors should be harvested"
    assert harvest_dates == [None, "2026-04-25T16:38:10"], (
        "recently updated author should omit previous harvest date while unupdatedauthor should keep it"
    )


def test_publications_from_orcid_omits_from_updated_date_when_harvest_date_is_none(
    monkeypatch,
):
    class MockAuthors:
        def filter(self, **kwargs):
            return self

        def get(self):
            return [{"id": "https://openalex.org/A123"}]

    class MockWorks:
        def __init__(self):
            self.filter_kwargs = None
            self.per_page = None

        def filter(self, **kwargs):
            self.filter_kwargs = kwargs
            return self

        def paginate(self, per_page=200):
            self.per_page = per_page
            yield []

    mock_works = MockWorks()

    monkeypatch.setattr(openalex, "Authors", lambda: MockAuthors())
    monkeypatch.setattr(openalex, "Works", lambda: mock_works)

    list(
        openalex.publications_from_orcid(
            "https://orcid.org/0000-0002-8030-5327", harvest_date=None
        )
    )

    assert mock_works.filter_kwargs == {"author": {"id": "https://openalex.org/A123"}}
    assert mock_works.per_page == 200


def test_publications_from_orcid_includes_from_updated_date_when_harvest_date_exists(
    monkeypatch,
):
    class MockAuthors:
        def filter(self, **kwargs):
            return self

        def get(self):
            return [{"id": "https://openalex.org/A123"}]

    class MockWorks:
        def __init__(self):
            self.filter_kwargs = None
            self.per_page = None

        def filter(self, **kwargs):
            self.filter_kwargs = kwargs
            return self

        def paginate(self, per_page=200):
            self.per_page = per_page
            yield []

    mock_works = MockWorks()
    harvest_date = "2026-04-27T16:38:10"

    monkeypatch.setattr(openalex, "Authors", lambda: MockAuthors())
    monkeypatch.setattr(openalex, "Works", lambda: mock_works)

    list(
        openalex.publications_from_orcid(
            "https://orcid.org/0000-0002-8030-5327", harvest_date=harvest_date
        )
    )

    assert mock_works.filter_kwargs == {
        "from_updated_date": harvest_date,
        "author": {"id": "https://openalex.org/A123"},
    }
    assert mock_works.per_page == 200


def test_harvest_stops_when_author_limit_is_exceeded(
    mock_incremental_authors,
    mock_rialto_db_name,
    caplog,
    monkeypatch,
):
    queried_orcids = []

    def _capture_orcid(orcid, harvest_date=None):
        # keep track of ORCID queries but doesn't return any publications (so that we're only testing the author limit)
        queried_orcids.append(orcid)
        yield from ()

    monkeypatch.setattr(openalex, "publications_from_orcid", _capture_orcid)

    caplog.set_level(logging.INFO)
    openalex.harvest(limit=1)

    assert queried_orcids == ["https://orcid.org/0000-0000-0000-0001"]
    assert "Reached limit of 1 publications or authors, stopping" in caplog.text
