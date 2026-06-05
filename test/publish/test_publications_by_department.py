import pytest
from sqlalchemy import select

from rialto_airflow.schema.rialto import Publication
from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import PublicationsByDepartment


@pytest.fixture
def rialto_db_name(monkeypatch):
    monkeypatch.setattr(publication, "RIALTO_DB_NAME", "rialto_incremental_test")


def test_write_publications_by_department(
    test_reports_session, dataset_incremental, rialto_db_name, caplog
):
    result = publication.export_publications_by_department()
    assert result == 1

    with test_reports_session.begin() as session:
        rows = session.execute(
            select(PublicationsByDepartment).order_by(
                PublicationsByDepartment.primary_department
            )
        ).all()
        assert len(rows) == 1

        row = rows[0][0]
        assert bool(row.academic_council_authored) is True
        assert row.apc == 123
        assert row.doi == "10.000/000001"
        assert bool(row.faculty_authored) is True
        assert bool(row.federally_funded) is False
        assert row.open_access == "gold"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"

        assert "started writing publications_by_department table" in caplog.text
        assert "finished writing publications_by_department table" in caplog.text


def test_limit_openalex_only(
    dataset_incremental,
    rialto_db_name,
    tmp_path,
    test_incremental_session,
    test_reports_session,
):
    # ensure one of the publications only has openalex metadata
    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        pub.sulpub_json = None
        pub.dim_json = None
        pub.wos_json = None
        pub.pubmed_json = None
        session.add(pub)
        session.flush()

    publication.export_publications_by_department()

    with test_reports_session.begin() as session:
        pubs = session.query(PublicationsByDepartment).all()
        assert len(pubs) == 0
