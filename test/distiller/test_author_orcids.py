from rialto_airflow.distiller import (
    author_list_orcids,
    first_author_orcid,
    last_author_orcid,
)
from rialto_airflow.schema.harvest import Publication


def test_author_orcids(pub_with_authors):
    # slowly peel away the platform metadata that's available to confirm that we are
    # matching in the right order, and looking for values correctly

    pub = pub_with_authors

    assert first_author_orcid(pub) == "jane-open-alex"
    assert last_author_orcid(pub) == "leland-open-alex"
    assert author_list_orcids(pub) == [
        "jane-crossref",
        "jane-dimensions",
        "jane-open-alex",
        "jane-pubmed",
        "jane-wos",
        "leland-crossref",
        "leland-dimensions",
        "leland-open-alex",
        "leland-pubmed",
        "leland-wos",
        "mike-crossref",
        "mike-dimensions",
        "mike-open-alex",
        "mike-pubmed",
        "mike-wos",
    ]

    pub.openalex_json = {}

    # dimensions
    assert first_author_orcid(pub) == "jane-dimensions"
    assert last_author_orcid(pub) == "leland-dimensions"
    assert author_list_orcids(pub) == [
        "jane-crossref",
        "jane-dimensions",
        "jane-pubmed",
        "jane-wos",
        "leland-crossref",
        "leland-dimensions",
        "leland-pubmed",
        "leland-wos",
        "mike-crossref",
        "mike-dimensions",
        "mike-pubmed",
        "mike-wos",
    ]

    pub.dim_json = {}

    # pubmed
    assert first_author_orcid(pub) == "jane-pubmed"
    assert last_author_orcid(pub) == "leland-pubmed"
    assert author_list_orcids(pub) == [
        "jane-crossref",
        "jane-pubmed",
        "jane-wos",
        "leland-crossref",
        "leland-pubmed",
        "leland-wos",
        "mike-crossref",
        "mike-pubmed",
        "mike-wos",
    ]

    pub.pubmed_json = {}

    # web-of-science
    assert first_author_orcid(pub) == "jane-wos"
    assert last_author_orcid(pub) == "leland-wos"
    assert author_list_orcids(pub) == [
        "jane-crossref",
        "jane-wos",
        "leland-crossref",
        "leland-wos",
        "mike-crossref",
        "mike-wos",
    ]

    pub.wos_json = {}

    # crossref
    assert first_author_orcid(pub) == "jane-crossref"
    assert last_author_orcid(pub) == "leland-crossref"
    assert author_list_orcids(pub) == [
        "jane-crossref",
        "leland-crossref",
        "mike-crossref",
    ]

    pub.crossref_json = {}

    # sulpub (we don't extract orcids from sulpub)
    assert first_author_orcid(pub) is None
    assert last_author_orcid(pub) is None
    assert author_list_orcids(pub) == []


def test_no_metadata():
    pub = Publication(
        doi="10.000/example",
        title="Help, I don't have any metadata!",
    )
    assert first_author_orcid(pub) is None
    assert last_author_orcid(pub) is None
    assert author_list_orcids(pub) == []


def test_pubmed_non_orcid():
    pub = Publication(
        doi="10.000/example",
        title="Help, I don't have any metadata!",
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "AuthorList": {
                        "Author": {
                            "ForeName": "Jane",
                            "LastName": "Pubmed",
                            "Identifier": {
                                "@Source": "SOCIAL",
                                "#text": "jane-pubmed",
                            },
                        }
                    }
                }
            }
        },
    )

    assert first_author_orcid(pub) is None
    assert last_author_orcid(pub) is None
    assert author_list_orcids(pub) == []


def test_one_author(one_author):
    assert first_author_orcid(one_author) == "jane-pubmed"
    assert last_author_orcid(one_author) == "jane-pubmed"

    # remove pubmed so that wos data is examined
    one_author.pubmed_json = {}

    assert first_author_orcid(one_author) == "jane-wos"
    assert last_author_orcid(one_author) == "jane-wos"


def test_pubmed_identifier_list():
    """
    PubMed authors sometimes have a list of identifiers instead of just having one.
    """
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        apc=None,
        open_access="gold",
        pub_year=2023,
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "AuthorList": {
                        "Author": {
                            "ForeName": "Jane",
                            "LastName": "Pubmed",
                            "Identifier": [
                                {
                                    "@Source": "ORCID",
                                    "#text": "jane-pubmed",
                                }
                            ],
                        }
                    }
                }
            }
        },
    )

    assert author_list_orcids(pub) == ["jane-pubmed"]
    assert first_author_orcid(pub) == "jane-pubmed"
    assert last_author_orcid(pub) == "jane-pubmed"
