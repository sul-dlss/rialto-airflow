import pytest

from rialto_airflow.publish import publication
from rialto_airflow.database import Publication, Author, Funder
from test.test_utils import TestRow


def dim_json():
    return {
        "journal": {"title": "Delicious Limes Journal of Science"},
        "issue": "12",
        "pages": "1-10",
        "volume": "1",
        "mesh_terms": ["Delicions", "Limes"],
        "pmid": "123",
        "linkout": "https://example_dim.com",
    }


def dim_json_no_title():
    return {"other_data": {"bogus": "no journal title here"}}


def openalex_json():
    return {
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
        "primary_location": {
            "source": {
                "type": "journal",
                "display_name": "Ok Limes Journal of Science",
            }
        },
        "locations": [
            {"pdf_url": "https://example_openalex_pdf.com"},
        ],
        "mesh": [
            {"descriptor_name": "Ok"},
            {"descriptor_name": "Lemons"},
        ],
        "ids": {
            "doi": "10.000/000001",
            "pmid": "1234",
        },
    }


def openalex_no_title_json():
    return {
        "locations": [
            {
                "bogus": {
                    "nope": "not here",
                }
            },
            {"source": {"type": "geography", "display_name": "New York"}},
        ],
    }


def wos_json():
    return {
        "static_data": {
            "summary": {
                "pub_info": {
                    "pubyear": "2023",
                    "issue": "10",
                    "vol": "3",
                    "page": {"begin": "1", "end": "8"},
                },
                "titles": {
                    "count": 2,
                    "title": [
                        {"type": "source", "content": "Meh Limes Journal of Science"},
                        {"type": "bogus", "content": "Bogus"},
                    ],
                },
            }
        },
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {
                    "identifier": [
                        {
                            "type": "pmid",
                            "value": "1234567",
                        }
                    ],
                },
            }
        },
    }


def wos_json_no_page_info():
    return {
        "static_data": {
            "summary": {
                "pub_info": {
                    "page": {"page_count": "7"},
                },
            }
        }
    }


def sulpub_json():
    return {
        "journal": {
            "name": "Bad Limes Journal of Science",
            "issue": "9",
            "pages": "1-7",
            "volume": "4",
        },
        "pmid": "123456",
    }


def pubmed_json():
    return {
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
    }


@pytest.fixture
def dataset(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000001",
            title="My Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json={"type": "article"},
            openalex_json={"type": "preprint"},
            wos_json={
                "static_data": {
                    "fullrecord_metadata": {
                        "normalized_doctypes": {"doctype": ["Article", "Abstract"]}
                    }
                }
            },
        )

        pub.authors.append(
            Author(
                first_name="Jane",
                last_name="Stanford",
                sunet="janes",
                cap_profile_id="1234",
                orcid="0298098343",
                primary_school="School of Humanities and Sciences",
                primary_dept="Social Sciences",
                primary_role="faculty",
                schools=[
                    "Vice Provost for Undergraduate Education",
                    "School of Humanities and Sciences",
                ],
                departments=["Inter-Departmental Programs", "Social Sciences"],
                academic_council=True,
            )
        )

        pub.authors.append(
            Author(
                first_name="Leland",
                last_name="Stanford",
                sunet="lelands",
                cap_profile_id="12345",
                orcid="02980983434",
                primary_school="School of Humanities and Sciences",
                primary_dept="Social Sciences",
                primary_role="staff",
                schools=[
                    "School of Humanities and Sciences",
                ],
                departments=["Social Sciences"],
                academic_council=False,
            )
        )

        pub.authors.append(
            Author(
                first_name="Frederick",
                last_name="Olmstead",
                sunet="folms",
                cap_profile_id="123456",
                orcid="02980983422",
                primary_school="School of Engineering",
                primary_dept="Mechanical Engineering",
                primary_role="faculty",
                schools=["School of Engineering"],
                departments=["Mechanical Engineering"],
                academic_council=False,
            )
        )

        pub.authors.append(
            Author(
                first_name="Frederick",
                last_name="Terman",
                sunet="fterm",
                cap_profile_id="1234567",
                orcid="029809834222",
                primary_school="School of Engineering",
                primary_dept="Electrical Engineering",
                primary_role="faculty",
                schools=["School of Engineering"],
                departments=["Electrical Engineering"],
                academic_council=True,
            )
        )

        pub.funders.append(
            Funder(name="National Institutes of Health", grid_id="12345", federal=True)
        )

        pub.funders.append(
            Funder(name="Andrew Mellon Foundation", grid_id="123456", federal=False)
        )

        session.add(pub)


def test_dataset(test_session, dataset):
    with test_session.begin() as session:
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub
        assert len(pub.authors) == 4
        assert len(pub.funders) == 2


def test_journal_with_dimensions_title():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    assert publication._journal(row) == "Delicious Limes Journal of Science"


def test_journal_missing_dimensions_title():
    row = TestRow(
        dim_json=dim_json_no_title(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_title():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_and_missing_open_alex_title():
    row = TestRow(
        openalex_json=openalex_no_title_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from wos
    assert publication._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_or_open_alex_title():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from wos
    assert publication._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_or_open_alex_or_wos_title():
    row = TestRow(
        sulpub_json=sulpub_json(),
    )
    # comes from sulpub
    assert publication._journal(row) == "Bad Limes Journal of Science"


def test_journal_with_dimensions_issue():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    assert publication._issue(row) == "12"


def test_journal_without_dimensions_issue():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._issue(row) == "11"


def test_journal_without_dimensions_or_openalex_issue():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._issue(row) == "10"


def test_journal_without_dimensions_or_openalex_or_wos_issue():
    row = TestRow(
        sulpub_json=sulpub_json(),
    )
    # comes from sulpub
    assert publication._issue(row) == "9"


def test_journal_with_dimensions_mesh():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from dimensions
    assert publication._mesh(row) == "Delicions|Limes"


def test_journal_without_dimensions_mesh():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._mesh(row) == "Ok|Lemons"


def test_journal_with_dimensions_pages():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from dimensions
    assert publication._pages(row) == "1-10"


def test_journal_without_dimensions_pages():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._pages(row) == "1-9"


def test_journal_without_dimensions_or_openalex_pages():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from wos
    assert publication._pages(row) == "1-8"


def test_journal_missing_wos_page_info():
    row = TestRow(
        wos_json=wos_json_no_page_info(),
        sulpub_json=sulpub_json(),
    )
    # comes from sulpub
    assert publication._pages(row) == "1-7"


def test_journal_without_dimensions_or_openalex_or_wos_pages():
    row = TestRow(
        sulpub_json=sulpub_json(),
    )
    # comes from sulpub
    assert publication._pages(row) == "1-7"


def test_journal_with_pubmed_pmid():
    row = TestRow(
        pubmed_json=pubmed_json(),
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from pubmed
    assert publication._pmid(row) == "36857419"


def test_journal_without_pubmed_pmid():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from dimensions
    assert publication._pmid(row) == "123"


def test_journal_without_pubmed_or_dimensions_pmid():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._pmid(row) == "1234"


def test_journal_without_pubmed_or_dimensions_or_openalex_pmid():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from sulpub
    assert publication._pmid(row) == "123456"


def test_journal_without_pubmed_or_dimensions_or_openalex_or_sulpub_pmid():
    row = TestRow(
        wos_json=wos_json(),
    )
    # comes from wos
    assert publication._pmid(row) == "1234567"


def test_journal_with_dimensions_url():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from dimensions
    assert publication._url(row) == "https://example_dim.com"


def test_journal_without_dimensions_url():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._url(row) == "https://example_openalex_pdf.com"


def test_journal_without_dimensions_or_openalex_url():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # no url
    assert publication._url(row) is None


def test_journal_with_dimensions_volume():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from dimensions
    assert publication._volume(row) == "1"


def test_journal_without_dimensions_volume():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from openalex
    assert publication._volume(row) == "2"


def test_journal_without_dimensions_or_openalex_volume():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    # comes from wos
    assert publication._volume(row) == "3"


def test_journal_without_dimensions_or_openalex_or_wos_volume():
    row = TestRow(
        sulpub_json=sulpub_json(),
    )
    # comes from sulpub
    assert publication._volume(row) == "4"
