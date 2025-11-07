from sqlalchemy import select

from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import PublicationsBySchool


def test_write_publications_by_school(test_reports_session, snapshot, dataset, caplog):
    # generate the publications_by_school table
    result = publication.export_publications_by_school(snapshot)
    assert result == 3

    with test_reports_session.begin() as session:
        rows = session.execute(
            select(PublicationsBySchool).order_by(PublicationsBySchool.primary_school)
        ).all()
        assert len(rows) == 3

        row = rows[0][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Engineering"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"

        row = rows[1][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "gold"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"

        row = rows[2][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 500
        assert row.doi == "10.000/000002"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is True
        assert row.open_access == "green"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.pub_year == 2024
        assert row.types == "Article|Preprint"

        assert "started writing publications_by_school table" in caplog.text
        assert "finished writing publications_by_school table" in caplog.text
