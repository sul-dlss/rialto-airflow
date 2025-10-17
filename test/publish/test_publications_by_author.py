import pytest
from sqlalchemy import select

from rialto_airflow.publish import publication
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
        assert row.journal_issn == "0009-4978|1234-5678|1523-8253|1943-5975"
        assert row.open_access == "gold"
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


def test_pubmed_abstract_text(pubmed_json_text):
    abstract = publication._pubmed_abstract(pubmed_json_text)
    assert abstract == "This is the abstract. It provides a summary of the article."
