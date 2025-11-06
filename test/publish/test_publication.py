from rialto_airflow.schema.harvest import Publication
from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import Publications


def test_dataset(test_session, dataset):
    with test_session.begin() as session:
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        pub2 = (
            session.query(Publication).where(Publication.doi == "10.000/000002").first()
        )
        assert pub.title == "My Life"
        assert pub2.title == "My Life Part 2"
        assert len(pub.authors) == 4
        assert len(pub.funders) == 2


def test_export_publications(test_reports_session, snapshot, dataset, caplog):
    # generate the publications table
    result = publication.export_publications(snapshot)
    assert result == 2

    with test_reports_session.begin() as session:
        assert session.query(Publications).count() == 2

    with test_reports_session.begin() as session:
        q = session.query(Publications).where(Publications.doi == "10.000/000001")
        db_rows = list(q.all())
        assert len(db_rows) == 1
        assert db_rows[0].apc == 123
        assert db_rows[0].types == "Article|Preprint"
        assert db_rows[0].open_access == "gold"
        assert db_rows[0].publisher == "Science Publisher Inc."
        assert (
            db_rows[0].journal_name
            == "Proceedings of the National Academy of Sciences of the United States of America"
        )
        assert bool(db_rows[0].academic_council_authored) is True
        assert bool(db_rows[0].faculty_authored) is True

    with test_reports_session.begin() as session:
        q = session.query(Publications).where(Publications.doi == "10.000/000002")
        db_rows = list(q.all())
        assert len(db_rows) == 1
        assert db_rows[0].apc == 500
        assert db_rows[0].types == "Article|Preprint"
        assert db_rows[0].open_access == "green"
        assert bool(db_rows[0].academic_council_authored) is True
        assert bool(db_rows[0].faculty_authored) is True

    assert "started writing publications table" in caplog.text
    assert "finished writing publications table" in caplog.text
