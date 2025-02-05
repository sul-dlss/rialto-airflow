import pickle
import re

import pandas
import pyalex
import pytest

from rialto_airflow.harvest import openalex


def test_dois_from_orcid():
    dois = list(openalex.dois_from_orcid("0000-0002-1298-3089"))
    assert len(dois) >= 54


def test_dois_from_orcid_paging():
    # per_page is set to 200, so ensure that paging is working
    # for Shanhui Fan who has a lot of publications (> 1300)
    dois = list(openalex.dois_from_orcid("0000-0002-0081-9732", limit=300))
    assert len(dois) == 300, "paging is limiting to 200 works"
    assert len(set(dois)) == len(dois), "the dois are unique"


def test_doi_orcids_pickle(tmp_path):
    # authors_csv, pickle_file):
    pickle_file = tmp_path / "openalex-doi-orcid.pickle"
    openalex.doi_orcids_pickle("test/data/authors.csv", pickle_file)
    assert pickle_file.is_file(), "created the pickle file"

    mapping = pickle.load(pickle_file.open("rb"))
    assert isinstance(mapping, dict)
    assert len(mapping) > 0

    doi = list(mapping.keys())[0]
    assert "https://doi.org/" not in doi, "doi is an ID"
    assert "/" in doi

    orcids = mapping[doi]
    assert isinstance(orcids, list)
    assert len(orcids) > 0
    assert re.match(r"^\d+-\d+-\d+-\d+$", orcids[0])


def test_publications_from_dois():
    # get 231 dois that we know are in openalex
    dois = pandas.read_csv("test/data/openalex-dois.csv").doi.to_list()
    assert len(dois) == 231

    # look up the publication metadata for them
    pubs = list(openalex.publications_from_dois(dois))

    # > 200 is used because some of the 231 DOIs have been removed from openalex 🤷
    assert len(pubs) > 200, "should paginate (page size=200)"
    assert set(openalex.FIELDS) == set(pubs[0].keys()), "All fields accounted for."
    assert len(pubs[0].keys()) == 54, "first publication has 54 columns"
    assert len(pubs[1].keys()) == 54, "second publication has 54 columns"


def test_publications_from_invalid_dois(caplog):
    invalid_dois = ["doi-with-comma,a", "10.1145/3442188.3445922"]
    assert len(list(openalex.publications_from_dois(invalid_dois))) == 1


def test_publications_from_invalid_with_comma(caplog):
    # OpenAlex will interpret a DOI string with a comma as two DOIs but
    # Does not return a result for the first half even if valid. Will return an empty list
    invalid_doi = ["10.1002/cncr.33546,-(wileyonlinelibrary.com)"]
    assert len(list(openalex.publications_from_dois(invalid_doi))) == 0


def test_publications_csv(tmp_path):
    pubs_csv = tmp_path / "openalex-pubs.csv"
    openalex.publications_csv(
        ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], pubs_csv
    )

    df = pandas.read_csv(pubs_csv)

    assert len(df) == 2

    # the order of the results isn't guaranteed but make sure things are coming back

    assert set(df.title.tolist()) == set(
        ["On the Dangers of Stochastic Parrots", "Attention Is All You Need"]
    )

    assert set(df.doi.tolist()) == set(
        [
            "https://doi.org/10.48550/arxiv.1706.03762",
            "https://doi.org/10.1145/3442188.3445922",
        ]
    )


def test_pyalex_urlencoding():
    assert pyalex.Works().filter(doi="10.1207/s15327809jls0703&4_2").count() == 1, (
        "url encoding the & works with OpenAlex API"
    )

    assert (
        len(
            list(
                openalex.publications_from_dois(
                    ["10.1207/s15327809jls0703&4_2", "10.1145/3442188.3445922"]
                )
            )
        )
        == 2
    ), "we handle URL encoding"


@pytest.mark.skip(reason="This record no longer exhibits the problem")
def test_pyalex_varnish_bug():
    # it seems like this author has a few records that are so big they blow out
    # OpenAlex's Varnish index. See https://groups.google.com/u/1/g/openalex-community/c/hl09WRF3Naw
    assert len(list(openalex.dois_from_orcid("0000-0003-3859-2905"))) > 270
