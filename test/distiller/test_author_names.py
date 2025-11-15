from rialto_airflow.distiller import (
    author_list_names,
    first_author_name,
    last_author_name,
)
from rialto_airflow.schema.harvest import Publication


def test_author_names(pub_with_authors):
    # slowly peel away the platform metadata that's available to confirm that we are
    # matching in the right order, and looking for values correctly

    pub = pub_with_authors

    assert author_list_names(pub) == [
        "Jane Open Alex",
        "Mike Open Alex",
        "Leland Open Alex",
    ]
    assert first_author_name(pub) == "Jane Open Alex"
    assert last_author_name(pub) == "Leland Open Alex"

    pub.openalex_json = {}

    # dimensions
    assert author_list_names(pub) == [
        "Jane Dimensions",
        "Mike Dimensions",
        "Leland Dimensions",
    ]
    assert first_author_name(pub) == "Jane Dimensions"
    assert last_author_name(pub) == "Leland Dimensions"

    pub.dim_json = {}

    # pubmed
    assert author_list_names(pub) == [
        "Jane Pubmed",
        "Mike Pubmed",
        "Leland Pubmed",
    ]
    assert first_author_name(pub) == "Jane Pubmed"
    assert last_author_name(pub) == "Leland Pubmed"

    pub.pubmed_json = {}

    # web-of-science
    assert author_list_names(pub) == ["Jane Wos", "Mike Wos", "Leland Wos"]
    assert first_author_name(pub) == "Jane Wos"
    assert last_author_name(pub) == "Leland Wos"

    pub.wos_json = {}

    # crossref
    assert author_list_names(pub) == [
        "Jane Crossref",
        "Mike Crossref",
        "Leland Crossref",
    ]
    assert first_author_name(pub) == "Jane Crossref"
    assert last_author_name(pub) == "Leland Crossref"

    pub.crossref_json = {}

    # sulpub (we don't extract orcids from sulpub)
    assert author_list_names(pub) == [
        "Jane Elizabeth Lathrop Sulpub",
        "Mike Sulpub",
        "Leland DeWitt Sulpub",
    ]
    assert first_author_name(pub) == "Jane Elizabeth Lathrop Sulpub"
    assert last_author_name(pub) == "Leland DeWitt Sulpub"


def test_no_metadata():
    pub = Publication(
        doi="10.000/example",
        title="Help, I don't have any metadata!",
    )
    assert author_list_names(pub) == []
    assert first_author_name(pub) is None
    assert last_author_name(pub) is None


def test_one_author(one_author):
    """
    Some platforms make make authors available as a list of objects, and as a
    single object (when there is only one author). These tests ensure that these
    are handled correctly.
    """

    assert author_list_names(one_author) == ["Jane Pubmed"]
    assert first_author_name(one_author) == "Jane Pubmed"
    assert last_author_name(one_author) == "Jane Pubmed"

    # remove pubmed so that wos data is examined
    one_author.pubmed_json = {}

    assert author_list_names(one_author) == ["Jane Wos"]
    assert first_author_name(one_author) == "Jane Wos"
    assert last_author_name(one_author) == "Jane Wos"


def test_crossref_missing_given_name():
    pub = Publication(
        doi="10.000/example",
        title="Help, I don't have any metadata!",
        crossref_json={
            "author": [
                {
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/jane-crossref",
                },
                {
                    "given": "Mike",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/mike-crossref",
                },
            ]
        },
    )

    assert author_list_names(pub) == ["Crossref", "Mike Crossref"]


def test_pubmed_missing_given_name():
    pub = Publication(
        doi="10.000/example",
        title="Help, I don't have any metadata!",
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "AuthorList": {
                        "Author": [
                            {
                                "LastName": "Pubmed",
                                "Identifier": {
                                    "@Source": "ORCID",
                                    "#text": "jane-pubmed",
                                },
                            },
                            {
                                "ForeName": "Mike",
                                "LastName": "Pubmed",
                                "Identifier": {
                                    "@Source": "ORCID",
                                    "#text": "mike-pubmed",
                                },
                            },
                        ]
                    }
                }
            }
        },
    )

    assert author_list_names(pub) == ["Pubmed", "Mike Pubmed"]
