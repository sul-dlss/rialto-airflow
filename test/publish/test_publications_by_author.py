import pytest
from sqlalchemy import select

from rialto_airflow.publish import publication
from rialto_airflow.schema.harvest import Publication
from rialto_airflow.schema.reports import PublicationsByAuthor


def test_write_publications_by_author(test_reports_session, snapshot, dataset, caplog):
    result = publication.export_publications_by_author(snapshot)
    assert result == 5

    with test_reports_session.begin() as session:
        rows = session.execute(
            select(PublicationsByAuthor).order_by(
                PublicationsByAuthor.doi, PublicationsByAuthor.sunet
            )
        ).all()
        assert len(rows) == 5

        row = rows[0][0]
        assert bool(row.academic_council) is False
        assert row.abstract == "This is an abstract which is inverted."
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.apc == 123
        assert row.citation_count == 100
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert (
            row.journal_name
            == "Proceedings of the National Academy of Sciences of the United States of America"
        )
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        # note: unlike the other first/last_author_orcid (which come from fixture publication JSON)
        # this orcid value comes from Author database model, which is different
        assert row.orcid == "02980983422"
        assert row.pages == "1-9"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Mechanical Engineering"
        assert row.publisher == "Science Publisher Inc."
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "folms"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[1][0]
        assert bool(row.academic_council) is True
        assert row.apc == 123
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.citation_count == 100
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        assert row.orcid == "029809834222"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Electrical Engineering"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "fterm"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[2][0]
        assert bool(row.academic_council) is True
        assert row.apc == 123
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        assert row.orcid == "0298098343"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "janes"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[3][0]
        assert bool(row.academic_council) is False
        assert row.apc == 123
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        assert row.orcid == "02980983434"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "staff"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "lelands"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[4][0]
        assert bool(row.academic_council) is True
        assert row.apc == 500
        assert row.doi == "10.000/000002"
        assert bool(row.federally_funded) is True
        assert row.open_access == "green"
        assert row.orcid == "0298098343"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "faculty"
        assert row.pages == "1-10"
        assert row.pub_year == 2024
        assert row.types == "Article|Preprint"
        assert row.sunet == "janes"
        assert row.title == "My Life Part 2"

        assert "started writing publications_by_author table" in caplog.text
        assert (
            "finished writing 5 rows to the publications_by_author table" in caplog.text
        )


def test_pubmed_abstract(pubmed_json):
    abstract = publication._pubmed_abstract(pubmed_json)
    assert (
        abstract
        == "Comorbid insomnia with obstructive sleep apnea (COMISA) is associated with worse daytime function and more medical/psychiatric comorbidities vs either condition alone. E2006-G000-304 was a phase 3, one-month polysomnography trial in adults aged ≥55 years with insomnia."
    )


@pytest.fixture
def pubmed_json_text():
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
                "Abstract": {
                    "AbstractText": [
                        "This is the abstract.",
                        "It provides a summary of the article.",
                    ]
                },
            }
        },
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": [
                    {"@IdType": "pubmed", "#text": "36857419"},
                    {"@IdType": "doi", "#text": "10.1182/bloodadvances.2022008893"},
                ]
            },
        },
    }


def test_pubmed_fields(pubmed_json_text):
    abstract = publication._pubmed_abstract(pubmed_json_text)
    assert abstract == "This is the abstract. It provides a summary of the article."


def test_pubmed_fields_no_abstract():
    pubmed_no_abstract = {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
            }
        },
    }

    abstract = publication._pubmed_abstract(pubmed_no_abstract)
    assert abstract is None


@pytest.fixture
def dim_json_fields():
    return {
        "type": "article",
        "doi": "10.000/000003",
        "issue": "12",
        "pages": "1-10",
        "volume": "1",
        "mesh_terms": ["Delicions", "Limes"],
        "pmid": "123",
        "linkout": "https://example_dim.com",
        "abstract": "This is a sample Dimensions abstract.",
    }


def test_dimensions_fields(test_session, dim_json_fields):
    # Add a publication with fields sourced from Dimensions
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My Dimensions Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=dim_json_fields,
            openalex_json=None,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
            publisher="Dimensions Publisher",
            journal_name="Delicious Limes Journal of Science",
        )
    session.add(pub)

    with test_session.begin() as select_session:
        result = select_session.execute(
            select(Publication).where(Publication.doi == "10.000/000003")
        )
        for row in result:
            assert publication._abstract(row) == "This is a sample Dimensions abstract."
            assert publication._pages(row) == "1-10"


def test_openalex_fields(test_session, openalex_json):
    # Add a publication with fields only sourced from OpenAlex
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My OpenAlex Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json=openalex_json,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
        session.add(pub)
        pub_row = session.query(Publication).filter_by(doi="10.000/000003").first()
        assert (
            publication._abstract(pub_row) == "This is an abstract which is inverted."
        )
        assert publication._pages(pub_row) == "1-9"


def test_rebuild_empty_abstract():
    openalex_json = {
        "id": "https://openalex.org/W123456789",
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
        "abstract_inverted_index": None,
    }
    abstract = publication._rebuild_abstract(openalex_json)
    assert abstract is None


def test_openalex_pages_start_only():
    openalex_json = {
        "biblio": {"issue": "11", "first_page": "1", "volume": "2"},
    }
    pages = publication._openalex_pages(openalex_json)
    assert pages == "1"


def test_openalex_pages_end_only():
    openalex_json = {
        "biblio": {"issue": "11", "last_page": "9", "volume": "2"},
    }
    pages = publication._openalex_pages(openalex_json)
    assert pages == "9"


def test_sulpub_fields(test_session, sulpub_json):
    # Add a publication with fields sourced from sulpub
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/sulpub",
            title="My Sulpub Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json=None,
            wos_json=None,
            sulpub_json=sulpub_json,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
        session.add(pub)

        pub_row = session.query(Publication).filter_by(doi="10.000/sulpub").first()
        assert publication._pages(pub_row) == "1-7"
        assert publication._citation_count(pub_row) is None


def test_volume_issue():
    """
    Test both volume and issue distill rules.
    """
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        openalex_json={"biblio": {"volume": "1", "issue": "2"}},
        dim_json={"volume": "3", "issue": "4"},
        pubmed_json={
            "MedlineCitation": {
                "Article": {"Journal": {"JournalIssue": {"Volume": "5", "Issue": "6"}}}
            }
        },
        sulpub_json={"journal": {"volume": "7", "issue": "8"}},
    )

    # slowly peel away the platform metadata that's available to confirm that we are
    # matching in the right order, and looking for values correctly

    assert publication._volume(pub) == "1"
    assert publication._issue(pub) == "2"

    pub.openalex_json = {}
    assert publication._volume(pub) == "3"
    assert publication._issue(pub) == "4"

    pub.dim_json = {}
    assert publication._volume(pub) == "5"
    assert publication._issue(pub) == "6"

    pub.pubmed_json = {}
    assert publication._volume(pub) == "7"
    assert publication._issue(pub) == "8"

    pub.sulpub_json = {}
    assert publication._volume(pub) is None
    assert publication._issue(pub) is None


def test_volume_issue_list():
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        openalex_json={"biblio": {"volume": ["24"], "issue": ["615"]}},
    )

    assert publication._volume(pub) == "24"
    assert publication._issue(pub) == "615"


def test_authors(test_session):
    """
    This test sets up some publication metadata and then calls various author
    metadata extraction functions with it to ensure the rules are working
    correctly.
    """
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        apc=None,
        open_access="gold",
        pub_year=2023,
        openalex_json={
            "authorships": [
                {
                    "author": {
                        "display_name": "Jane Open Alex",
                        "orcid": "jane-open-alex",
                    }
                },
                {
                    "author": {
                        "display_name": "Mike Open Alex",
                        "orcid": "mike-open-alex",
                    }
                },
                {
                    "author": {
                        "display_name": "Leland Open Alex",
                        "orcid": "leland-open-alex",
                    }
                },
            ]
        },
        dim_json={
            "authors": [
                {
                    "first_name": "Jane",
                    "last_name": "Dimensions",
                    "orcid": ["jane-dimensions"],
                },
                {
                    "first_name": "Mike",
                    "last_name": "Dimensions",
                    "orcid": ["mike-dimensions"],
                },
                {
                    "first_name": "Leland",
                    "last_name": "Dimensions",
                    "orcid": ["leland-dimensions"],
                },
            ]
        },
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "AuthorList": {
                        "Author": [
                            {
                                "ForeName": "Jane",
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
                            {
                                "ForeName": "Leland",
                                "LastName": "Pubmed",
                                "Identifier": {
                                    "@Source": "ORCID",
                                    "#text": "leland-pubmed",
                                },
                            },
                        ]
                    }
                }
            }
        },
        wos_json={
            "static_data": {
                "summary": {
                    "names": {
                        "name": [
                            {"display_name": "Jane Wos", "orcid_id": "jane-wos"},
                            {"display_name": "Mike Wos", "orcid_id": "mike-wos"},
                            {"display_name": "Leland Wos", "orcid_id": "leland-wos"},
                        ]
                    }
                }
            }
        },
        crossref_json={
            "author": [
                {
                    "given": "Jane",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/jane-crossref",
                },
                {
                    "given": "Mike",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/mike-crossref",
                },
                {
                    "given": "Leland",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/leland-crossref",
                },
            ]
        },
        sulpub_json={
            "author": [
                {"name": "Sulpub, Jane Elizabeth Lathrop"},
                {"name": "Sulpub, Mike"},
                {"name": "Sulpub, Leland DeWitt"},
            ]
        },
    )

    # slowly peel away the platform metadata that's available to confirm that we are
    # matching in the right order, and looking for values correctly

    assert publication._author_list_names(pub) == [
        "Jane Open Alex",
        "Mike Open Alex",
        "Leland Open Alex",
    ]
    assert publication._first_author_name(pub) == "Jane Open Alex"
    assert publication._last_author_name(pub) == "Leland Open Alex"
    assert publication._first_author_orcid(pub) == "jane-open-alex"
    assert publication._last_author_orcid(pub) == "leland-open-alex"
    assert publication._author_list_orcids(pub) == [
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
    assert publication._author_list_names(pub) == [
        "Jane Dimensions",
        "Mike Dimensions",
        "Leland Dimensions",
    ]
    assert publication._first_author_name(pub) == "Jane Dimensions"
    assert publication._last_author_name(pub) == "Leland Dimensions"
    assert publication._first_author_orcid(pub) == "jane-dimensions"
    assert publication._last_author_orcid(pub) == "leland-dimensions"
    assert publication._author_list_orcids(pub) == [
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
    assert publication._author_list_names(pub) == [
        "Jane Pubmed",
        "Mike Pubmed",
        "Leland Pubmed",
    ]
    assert publication._first_author_name(pub) == "Jane Pubmed"
    assert publication._last_author_name(pub) == "Leland Pubmed"
    assert publication._first_author_orcid(pub) == "jane-pubmed"
    assert publication._last_author_orcid(pub) == "leland-pubmed"
    assert publication._author_list_orcids(pub) == [
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
    assert publication._author_list_names(pub) == ["Jane Wos", "Mike Wos", "Leland Wos"]
    assert publication._first_author_name(pub) == "Jane Wos"
    assert publication._last_author_name(pub) == "Leland Wos"
    assert publication._first_author_orcid(pub) == "jane-wos"
    assert publication._last_author_orcid(pub) == "leland-wos"
    assert publication._author_list_orcids(pub) == [
        "jane-crossref",
        "jane-wos",
        "leland-crossref",
        "leland-wos",
        "mike-crossref",
        "mike-wos",
    ]

    pub.wos_json = {}

    # crossref
    assert publication._author_list_names(pub) == [
        "Jane Crossref",
        "Mike Crossref",
        "Leland Crossref",
    ]
    assert publication._first_author_name(pub) == "Jane Crossref"
    assert publication._last_author_name(pub) == "Leland Crossref"
    assert publication._first_author_orcid(pub) == "jane-crossref"
    assert publication._last_author_orcid(pub) == "leland-crossref"
    assert publication._author_list_orcids(pub) == [
        "jane-crossref",
        "leland-crossref",
        "mike-crossref",
    ]

    pub.crossref_json = {}

    # sulpub (we don't extract orcids from sulpub)
    assert publication._author_list_names(pub) == [
        "Jane Elizabeth Lathrop Sulpub",
        "Mike Sulpub",
        "Leland DeWitt Sulpub",
    ]
    assert publication._first_author_name(pub) == "Jane Elizabeth Lathrop Sulpub"
    assert publication._last_author_name(pub) == "Leland DeWitt Sulpub"
    assert publication._first_author_orcid(pub) is None
    assert publication._last_author_orcid(pub) is None
    assert publication._author_list_orcids(pub) == []


def test_authors_no_metadata():
    pub = Publication(
        doi="10.000/example",
        title="Help, I don't have any metadata!",
    )
    assert publication._author_list_names(pub) == []
    assert publication._first_author_name(pub) is None
    assert publication._last_author_name(pub) is None
    assert publication._first_author_orcid(pub) is None
    assert publication._last_author_orcid(pub) is None
    assert publication._author_list_orcids(pub) == []


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

    assert publication._first_author_orcid(pub) is None
    assert publication._last_author_orcid(pub) is None
    assert publication._author_list_orcids(pub) == []


def test_one_author():
    """
    Some platforms make make authors available as a list of objects, and as a
    single object (when there is only one author). These tests ensure that these
    are handled correctly.
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
                            "Identifier": {
                                "@Source": "ORCID",
                                "#text": "jane-pubmed",
                            },
                        }
                    }
                }
            }
        },
        wos_json={
            "static_data": {
                "summary": {
                    "names": {
                        "name": {"display_name": "Jane Wos", "orcid_id": "jane-wos"}
                    }
                }
            }
        },
    )

    assert publication._author_list_names(pub) == ["Jane Pubmed"]
    assert publication._first_author_name(pub) == "Jane Pubmed"
    assert publication._last_author_name(pub) == "Jane Pubmed"
    assert publication._first_author_orcid(pub) == "jane-pubmed"

    # remove pubmed so that wos data is examined
    pub.pubmed_json = {}

    assert publication._author_list_names(pub) == ["Jane Wos"]
    assert publication._first_author_name(pub) == "Jane Wos"
    assert publication._last_author_name(pub) == "Jane Wos"
    assert publication._first_author_orcid(pub) == "jane-wos"


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

    assert publication._author_list_orcids(pub) == ["jane-pubmed"]
    assert publication._first_author_orcid(pub) == "jane-pubmed"
    assert publication._last_author_orcid(pub) == "jane-pubmed"


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

    assert publication._author_list_names(pub) == ["Crossref", "Mike Crossref"]


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

    assert publication._author_list_names(pub) == ["Pubmed", "Mike Pubmed"]
