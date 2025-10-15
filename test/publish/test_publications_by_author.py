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
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Mechanical Engineering"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"
        assert row.sunet == "folms"

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

        assert "started writing publications_by_author table" in caplog.text
        assert (
            "finished writing 5 rows to the publications_by_author table" in caplog.text
        )
