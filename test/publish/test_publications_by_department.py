from sqlalchemy import select

from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import PublicationsByDepartment


def test_write_publications_by_department(
    test_reports_session, snapshot, dataset, caplog
):
    result = publication.export_publications_by_department(snapshot)
    assert result == 4

    with test_reports_session.begin() as session:
        rows = session.execute(
            select(PublicationsByDepartment).order_by(
                PublicationsByDepartment.primary_department
            )
        ).all()
        assert len(rows) == 4

        row = rows[0][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Electrical Engineering"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"

        row = rows[1][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Mechanical Engineering"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"

        row = rows[2][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.pub_year == 2023
        assert row.types == "article|preprint"

        row = rows[3][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 500
        assert row.doi == "10.000/000002"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "green"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.pub_year == 2024
        assert row.types == "article|preprint"

        assert "started writing publications_by_department table" in caplog.text
        assert "finished writing publications_by_department table" in caplog.text
