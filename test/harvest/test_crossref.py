import logging

import dotenv
import pandas

from rialto_airflow.database import Publication
from rialto_airflow.harvest import crossref
from test.utils import num_jsonl_objects

dotenv.load_dotenv()


def test_get_dois():
    """
    Test our Crossref API lookups by DOI. Note, we store the DOIs without the
    'doi:' prefix, however the get_dois() function will add them as needed since
    the API requires them.
    """
    # get 25 DOIs
    df = pandas.read_csv("test/data/dois.csv")
    dois = list(df.doi[0:100])

    # look them up
    results = crossref.get_dois(dois)

    # see if they look good
    assert len(list(results)) == 93  # 7 are invalid DOIs
    for result in results:
        assert "DOI" in result


def test_get_dois_missing():
    """
    Test that things work when looking up bogus DOIs.
    """
    df = pandas.read_csv("test/data/dois.csv")
    dois = [f"{doi}-naw" for doi in df.doi[0:25]]

    results = crossref.get_dois(dois)
    assert len(list(results)) == 0


def test_invalid_doi_prefix(caplog):
    """
    Test lookup with invalid DOI format. According to the API docs a DOI must
    match doi:10.prefix/suffix where prefix is of length 4 or more.
    """
    results = crossref.get_dois(["10.123/abcdef"])
    assert len(list(results)) == 0, ".123 prefix is too short"
    assert "Ignoring doi:10.123/abcdef with invalid prefix code 123" in caplog.text
    assert "No valid DOIs to look up" in caplog.text


def test_non_numeric_prefix(caplog):
    """
    The DOI prefix must be a number.
    """
    results = crossref.get_dois(["10.123a/abcdef"])
    assert len(list(results)) == 0, "ignore dois with non-numeric prefix"
    assert "Ignoring invalid DOI format doi:10.123a/abcdef" in caplog.text


def test_doi_missing_10(caplog):
    """
    DOI must start with "10."
    """
    assert len(list(crossref.get_dois(["1234/abcdef"]))) == 0, "missing 10."
    assert "Ignoring invalid DOI format doi:1234/abcdef" in caplog.text


def test_doi_missing_suffix(caplog):
    """
    DOIs must have a "/" followed by a string.
    """
    assert len(list(crossref.get_dois(["10.2345"]))) == 0, "missing /suffix"
    assert "Ignoring invalid DOI format doi:10.2345" in caplog.text


def test_fill_in(snapshot, test_session, mock_publication, caplog, monkeypatch):
    caplog.set_level(logging.INFO)

    # setup Works to return a list of one record
    records = [
        {
            "DOI": "10.1515/9781503624153",
            "title": "A sample title",
            "publication_year": 1891,
        }
    ]
    monkeypatch.setattr(crossref, "get_dois", lambda _: records)

    crossref.fill_in(snapshot)

    with test_session.begin() as session:
        pub = (
            session.query(Publication)
            .where(Publication.doi == "10.1515/9781503624153")
            .first()
        )
        assert pub.crossref_json == {
            "DOI": "10.1515/9781503624153",
            "title": "A sample title",
            "publication_year": 1891,
        }

    # adds 1 publication to the jsonl file
    assert num_jsonl_objects(snapshot.path / "crossref.jsonl") == 1
    assert "filled in 1 publications" in caplog.text
