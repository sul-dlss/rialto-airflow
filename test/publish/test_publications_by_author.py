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
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.journal_issn == "0009-4978|1234-0000|1234-5678|1523-8253|1943-5975"
        assert (
            row.journal_name
            == "Proceedings of the National Academy of Sciences of the United States of America"
        )
        assert row.open_access == "gold"
        assert row.pages == "1-9"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Mechanical Engineering"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"
        assert row.sunet == "folms"
        assert row.title == "My Life"

        row = rows[1][0]
        assert bool(row.academic_council) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Electrical Engineering"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"
        assert row.sunet == "fterm"
        assert row.title == "My Life"

        row = rows[2][0]
        assert bool(row.academic_council) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"
        assert row.sunet == "janes"
        assert row.title == "My Life"

        row = rows[3][0]
        assert bool(row.academic_council) is False
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "staff"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"
        assert row.sunet == "lelands"
        assert row.title == "My Life"

        row = rows[4][0]
        assert bool(row.academic_council) is True
        assert row.apc == 500
        assert row.doi == "10.000/000002"
        assert bool(row.federally_funded) is True
        assert row.open_access == "green"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "faculty"
        assert row.pages == "1-10"
        assert row.pub_year == 2024
        assert row.types == "article|preprint"
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
                    "Title": "Example Journal",
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


def test_pubmed_fields(pubmed_json_text, test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My PubMed Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json=None,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=pubmed_json_text,
            crossref_json=None,
            types=["article", "preprint"],
        )
    session.add(pub)

    with test_session.begin() as select_session:
        result = select_session.execute(
            select(Publication).where(Publication.doi == "10.000/000003")
        )
        for row in result:
            abstract = publication._pubmed_abstract(row)
            assert (
                abstract
                == "This is the abstract. It provides a summary of the article."
            )
            journal_name = publication._journal_name(row)
            assert journal_name == "Example Journal"


@pytest.fixture
def dim_json_fields():
    return {
        "type": "article",
        "doi": "10.000/000003",
        "journal": {"title": "Delicious Limes Journal of Science"},
        "issn": "1111-2222",
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
        )
    session.add(pub)

    with test_session.begin() as select_session:
        result = select_session.execute(
            select(Publication).where(Publication.doi == "10.000/000003")
        )
        for row in result:
            assert publication._abstract(row) == "This is a sample Dimensions abstract."
            assert publication._journal_issn(row) == "1111-2222"
            assert publication._pages(row) == "1-10"


def test_openalex_fields(test_session, openalex_json):
    # primary_location.issn_l is tested via dataset. Set up json to test other JsonRule for OpenAlex ISSNs
    openalex_json["primary_location"]["issn_l"] = None

    # Add a publication with fields only sourced from OpenAlex
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My OpenAlex Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=dim_json_fields,
            openalex_json=openalex_json,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
    session.add(pub)

    with test_session.begin() as select_session:
        result = select_session.execute(
            select(Publication).where(Publication.doi == "10.000/000003")
        )
        for row in result:
            assert (
                publication._abstract(row) == "This is an abstract which is inverted."
            )
            assert publication._journal_issn(row) == "0009-4978|1523-8253|1943-5975"
            assert publication._pages(row) == "123-130"


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
            doi="10.000/000003",
            title="My Dimensions Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            wos_json=None,
            sulpub_json=sulpub_json,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
    session.add(pub)

    with test_session.begin() as select_session:
        result = select_session.execute(
            select(Publication).where(Publication.doi == "10.000/000003")
        )
        for row in result:
            assert publication._journal_issn(row) == "1111-2222"
            assert publication._pages(row) == "1-7"
