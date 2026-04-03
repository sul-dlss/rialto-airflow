import logging
import pytest
import re
import dotenv

from rialto_airflow.schema.harvest import Publication
from rialto_airflow.harvest import pubmed
from test.utils import num_jsonl_objects, load_jsonl_file, num_log_record_matches

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
def mock_pubmed_search_no_results(monkeypatch):
    """
    Mock our function for searching for PMIDs given an ORCID
    """

    def f(*args, **kwargs):
        return []

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
    caplog.set_level(logging.DEBUG)

    requests_mock.get(
        re.compile(".*"),
        json={"header": {"type": "esearch", "version": "0.3"}},
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    assert pubmed.pmids_from_orcid("nope") == []
    assert (
        num_log_record_matches(
            caplog.records,
            logging.DEBUG,
            "No esearchresult or count found for nope[auid]",
        )
        == 1
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
    assert num_jsonl_objects(snapshot.path / "pubmed.jsonl") == 4

    # make sure a publication is in the database and linked to the author
    with test_session.begin() as session:
        assert session.query(Publication).count() == 2, "two publications loaded"

        pubs = session.query(Publication).all()
        assert pubs[0].doi == "10.1182/bloodadvances.2022008893", "doi was added"
        assert pubs[1].doi == "10.1200/jco.22.01076", "doi was normalized"

        assert len(pubs[0].authors) == 2, "publication has two authors"
        assert pubs[0].authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"
        assert pubs[0].authors[1].orcid == "https://orcid.org/0000-0000-0000-0002"


def test_harvest_limit(
    snapshot, test_session, mock_authors, mock_pubmed_fetch, mock_pubmed_search, caplog
):
    """
    With some authors loaded and a mocked Pubmed API and an artificially low
    harvest limit, confirm that processing stops as expected and logs appropriately.
    """
    # harvest from Pubmed with a limit of one publication
    pubmed.harvest(snapshot, 1)

    # the mocked Pubmed api returns the same two publications for both authors, but we
    # only process the first
    assert num_jsonl_objects(snapshot.path / "pubmed.jsonl") == 1

    # make sure a publication is in the database and linked to the author
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, "only one publication loaded"

        pubs = session.query(Publication).all()
        assert pubs[0].doi == "10.1182/bloodadvances.2022008893", "doi was added"

        assert len(pubs[0].authors) == 1, (
            "publication has one author because limit is reached before second is processed"
        )
        assert pubs[0].authors[0].orcid == "https://orcid.org/0000-0000-0000-0001"

        assert (
            num_log_record_matches(
                caplog.records,
                logging.WARNING,
                "Reached limit of 1 publications stopping",
            )
            == 1
        )


def test_harvest_no_pubmed_results(
    snapshot,
    test_session,
    mock_authors,
    mock_pubmed_fetch,
    mock_pubmed_search_no_results,
    caplog,
):
    """
    With some authors loaded and a Pubmed API mocked to never find anything, confirm that we
    log the unsuccessful searches appropriately.
    """
    caplog.set_level(logging.DEBUG)
    pubmed.harvest(snapshot)
    assert num_jsonl_objects(snapshot.path / "pubmed.jsonl") == 0
    with test_session.begin() as session:
        assert session.query(Publication).count() == 0, (
            "no publications loaded because none found"
        )
        assert (
            num_log_record_matches(
                caplog.records,
                logging.DEBUG,
                "No publications found for https://orcid.org/0000-0000-0000-0001",
            )
            == 1
        )
        assert (
            num_log_record_matches(
                caplog.records,
                logging.DEBUG,
                "No publications found for https://orcid.org/0000-0000-0000-0002",
            )
            == 1
        )


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
    assert num_jsonl_objects(snapshot.path / "pubmed.jsonl") == 4

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
    pubmed.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.pubmed_json == pubmed_json_fill_in_doi()

    # adds 1 publication to the jsonl file
    assert num_jsonl_objects(snapshot.path / "pubmed-fillin.jsonl") == 1
    assert "filled in 1 publications" in caplog.text


def test_fill_in_no_pubmed(
    test_session, mock_publication, snapshot, caplog, monkeypatch
):
    caplog.set_level(logging.INFO)

    # mock pubmed api to return no records for the doi
    monkeypatch.setattr(pubmed, "pmids_from_dois", lambda *args, **kwargs: [])
    pubmed.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.pubmed_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "pubmed-fillin.jsonl") == 0
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
    pubmed.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.pubmed_json is None

    # adds 0 publications to the jsonl file
    assert num_jsonl_objects(snapshot.path / "pubmed-fillin.jsonl") == 0
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


def test_pubs_from_pmids_no_articles(requests_mock, pubmed_book_xml):
    """
    Test that an empty list is returned when no articles are found for a PMIDs
    """
    requests_mock.post(
        re.compile(".*"),
        text=pubmed_book_xml,
        status_code=200,
        headers={"Content-Type": "application/xml"},
    )
    pubs = pubmed.publications_from_pmids(["00000000"])
    assert pubs == []


@pytest.fixture
def pubmed_book_xml():
    return """<?xml version="1.0" ?>
        <!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2025//EN" "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_250101.dtd">
        <PubmedArticleSet><PubmedBookArticle>
        <BookDocument>
            <PMID Version="1">38478703</PMID>
            <ArticleIdList><ArticleId IdType="bookaccession">NBK601514</ArticleId><ArticleId IdType="doi">10.25302/01.2021.ME.150731469</ArticleId></ArticleIdList>
            <Book>
            <Publisher><PublisherName>Patient-Centered Outcomes Research Institute (PCORI)</PublisherName><PublisherLocation>Washington (DC)</PublisherLocation></Publisher>
            <BookTitle book="pcori12021me15073146">Comparing Preferences for Depression and Diabetes Treatment among Adults of Different Racial and Ethnic Groups Who Reported Discrimination in Health Care</BookTitle>
            <PubDate><Year>2021</Year><Month>01</Month></PubDate>
            <CollectionTitle book="pcoricollect">PCORI Final Research Reports</CollectionTitle>
            <ELocationID EIdType="doi">10.25302/01.2021.ME.150731469</ELocationID>
            <PublicationType UI="D016454">Review</PublicationType>
            <Abstract><AbstractText Label="BACKGROUND">The abstract.</AbstractText></Abstract>
            </Book>
        </BookDocument>
        <PubmedBookData>
            <ArticleIdList><ArticleId IdType="pubmed">38478703</ArticleId></ArticleIdList></PubmedBookData>
        </PubmedBookArticle></PubmedArticleSet>
        """


def test_retry_500():
    pass
    # 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&retmax=1000&api_key=265ff40b3b20f36797d229d6baefd235ef09&term=%28%2210.1097%2Fypg.0b013e3283539517%22%5Bdoi%5D+OR+%2210.1126%2Fscisignal.2002568%22%5Bdoi%5D+OR+%2210.1111%2Fj.1558-5646.2011.01552.x%22%5Bdoi%5D+OR+%2210.13031%2F2013.24047%22%5Bdoi%5D+OR+%2210.1093%2Fbioinformatics%2Fbts336%22%5Bdoi%5D+OR+%2210.1201%2F9780203473689%22%5Bdoi%5D+OR+%2210.1063%2F1.2721130%22%5Bdoi%5D+OR+%2210.1038%2Fsc.1993.95%22%5Bdoi%5D+OR+%2210.1177%2F0305735603031002294%22%5Bdoi%5D+OR+%2210.1016%2Fj.amjmed.2015.12.027%22%5Bdoi%5D+OR+%2210.1002%2Fanie.201203035%22%5Bdoi%5D+OR+%2210.1016%2Fj.hrthm.2014.06.030%22%5Bdoi%5D+OR+%2210.1186%2Fs13073-020-00748-z%22%5Bdoi%5D+OR+%2210.3109%2F10428194.2012.701009%22%5Bdoi%5D+OR+%2210.1523%2Fjneurosci.5369-11.2012%22%5Bdoi%5D+OR+%2210.1145%2F1013115.1013148%22%5Bdoi%5D+OR+%2210.1016%2Fj.echo.2012.05.004%22%5Bdoi%5D+OR+%2210.1371%2Fjournal.pone.0037984%22%5Bdoi%5D+OR+%2210.1016%2Fj.jacc.2011.12.049%22%5Bdoi%5D+OR+%2210.1016%2Fj.bbcan.2012.05.004%22%5Bdoi%5D+OR+%2210.1002%2Fbit.10835%22%5Bdoi%5D+OR+%2210.1016%2Fj.bpj.2012.01.063%22%5Bdoi%5D+OR+%2210.1016%2Fj.bpj.2012.03.072%22%5Bdoi%5D+OR+%2210.1016%2Fj.jaad.2012.04.013%22%5Bdoi%5D+OR+%2210.1159%2F000336391%22%5Bdoi%5D+OR+%2210.1016%2Fj.cmet.2012.03.018%22%5Bdoi%5D+OR+%2210.1111%2Fj.1600-6143.2012.04127.x%22%5Bdoi%5D+OR+%2210.1016%2Fj.amjcard.2013.09.045%22%5Bdoi%5D+OR+%2210.1063%2F1.3669440%22%5Bdoi%5D+OR+%2210.1016%2Fj.dcn.2011.09.007%22%5Bdoi%5D+OR+%2210.1016%2Fj.molcel.2012.05.007%22%5Bdoi%5D+OR+%2210.1016%2Fj.molimm.2012.05.004%22%5Bdoi%5D+OR+%2210.2139%2Fssrn.1587986%22%5Bdoi%5D+OR+%2210.1038%2Fncomms1867%22%5Bdoi%5D+OR+%2210.3109%2F08820139.2012.692418%22%5Bdoi%5D+OR+%2210.1097%2Fmlr.0b013e3182585355%22%5Bdoi%5D+OR+%2210.1161%2Fcircresaha.115.306723%22%5Bdoi%5D+OR+%2210.1002%2Fhumu.23760%22%5Bdoi%5D+OR+%2210.1021%2Fja0449981%22%5Bdoi%5D+OR+%2210.1016%2Fj.jval.2023.09.486%22%5Bdoi%5D+OR+%2210.1128%2Faac.06015-11%22%5Bdoi%5D+OR+%2210.1159%2F000445241%22%5Bdoi%5D+OR+%2210.1128%2Fmcb.00432-12%22%5Bdoi%5D+OR+%2210.1016%2Fj.ahj.2013.10.027%22%5Bdoi%5D+OR+%2210.1073%2Fpnas.1115571109%22%5Bdoi%5D+OR+%2210.1063%2F1.3677935%22%5Bdoi%5D+OR+%2210.1634%2Ftheoncologist.2012-0041%22%5Bdoi%5D+OR+%2210.1007%2Fs10554-015-0804-x%22%5Bdoi%5D+OR+%2210.1149%2F1.3000002%22%5Bdoi%5D+OR+%2210.1097%2Frti.0b013e318255002c%22%5Bdoi%5D%29'
