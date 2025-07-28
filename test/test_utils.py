import csv
from pathlib import Path

import pytest

from rialto_airflow import utils


@pytest.fixture
def authors_csv(tmp_path):
    # Create a fixture authors CSV file
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sunetid", "orcidid"])
        writer.writerow(["author1", "https://orcid.org/0000-0000-0000-0001"])
        writer.writerow(["author2", ""])
        writer.writerow(["author3", "https://orcid.org/0000-0000-0000-0002"])
    return fixture_file


def test_rialto_authors_file():
    csv_file = utils.rialto_authors_file("test/data")
    assert Path(csv_file).is_file()

    with pytest.raises(Exception):
        utils.rialto_authors_file("/no/authors/file/here")


def test_rialto_active_authors_file():
    csv_file = utils.rialto_active_authors_file("test/data")
    assert Path(csv_file).is_file()

    with pytest.raises(Exception):
        utils.rialto_active_authors_file("/no/authors/file/here")


def test_normalize_doi():
    assert utils.normalize_doi("https://doi.org/10.1234/5678") == "10.1234/5678"
    assert utils.normalize_doi("https://dx.doi.org/10.1234/5678") == "10.1234/5678"
    assert (
        utils.normalize_doi("10.1103/PhysRevLett.96.07390")
        == "10.1103/physrevlett.96.07390"
    )
    assert utils.normalize_doi(" 10.1234/5678 ") == "10.1234/5678"
    assert utils.normalize_doi(" doi: 10.1234/5678 ") == "10.1234/5678"
    assert utils.normalize_doi("doi:10.1234/5678") == "10.1234/5678"
    assert utils.normalize_doi("doi:10.1234/ 56 78") == "10.1234/5678"
    assert utils.normalize_doi(None) is None


def test_normalize_pmid():
    assert utils.normalize_pmid("https://pubmed.ncbi.nlm.nih.gov/3685741") == "3685741"
    assert utils.normalize_pmid("MEDLINE:3685741") == "3685741"
    assert utils.normalize_pmid(" 3685741 ") == "3685741"
    assert utils.normalize_pmid("3685741") == "3685741"
    assert utils.normalize_pmid("") == ""
    assert utils.normalize_pmid(None) is None


def test_normalize_orcid():
    assert (
        utils.normalize_orcid("https://orcid.org/0000-0002-7262-6251")
        == "0000-0002-7262-6251"
    )
    assert (
        utils.normalize_orcid("https://sandbox.orcid.org/0000-0002-7262-6251")
        == "0000-0002-7262-6251"
    )
    assert utils.normalize_orcid("0000-0002-7262-6251") == "0000-0002-7262-6251"
    assert (
        utils.normalize_orcid(" HTTPS://ORCID.org/0000-0002-7262-6251 ")
        == "0000-0002-7262-6251"
    )
