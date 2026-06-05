import pytest
import zipfile
import datetime

from rialto_airflow.schema.rialto import Publication, Harvest
from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import Publications


def test_dataset(test_incremental_session, dataset_incremental):
    with test_incremental_session.begin() as session:
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub.title == "My Life"
        assert len(pub.authors) == 2
        assert len(pub.funders) == 0


def test_export_publications(test_reports_session, dataset_incremental, caplog):

    # generate the publications table
    result = publication.export_publications()
    assert result == 1

    with test_reports_session.begin() as session:
        assert session.query(Publications).count() == 1

    with test_reports_session.begin() as session:
        q = session.query(Publications).where(Publications.doi == "10.000/000001")
        db_rows = list(q.all())
        assert len(db_rows) == 1
        assert db_rows[0].apc == 123
        assert db_rows[0].author_list_names == "Jane Stanford|Leland Stanford"
        assert db_rows[0].types == "Article|Preprint"
        assert db_rows[0].open_access == "gold"
        assert db_rows[0].publisher == "Science Publisher Inc."
        assert (
            db_rows[0].journal_name
            == "Proceedings of the National Academy of Sciences of the United States of America"
        )
        assert db_rows[0].title == "My Life"
        assert bool(db_rows[0].academic_council_authored) is True
        assert bool(db_rows[0].faculty_authored) is True

    assert "started writing publications table" in caplog.text
    assert "finished writing publications table" in caplog.text


def test_generate_download_files(tmp_path, test_reports_session, dataset_incremental):
    # create the reports database
    publication.export_publications()

    # generate the download files
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()
    publication.generate_download_files(tmp_path)

    # check that the expected files were created and CSVs removed
    publications_file = downloads_dir / "publications.zip"
    assert publications_file.is_file()
    assert downloads_dir / "publications.csv" not in downloads_dir.iterdir()

    publications_by_school_file = downloads_dir / "publications_by_school.zip"
    assert publications_by_school_file.is_file()
    assert downloads_dir / "publications_by_school.csv" not in downloads_dir.iterdir()

    publications_by_department_file = downloads_dir / "publications_by_department.zip"
    assert publications_by_department_file.is_file()
    assert (
        downloads_dir / "publications_by_department.csv" not in downloads_dir.iterdir()
    )

    publications_by_author_file = downloads_dir / "publications_by_author.zip"
    assert publications_by_author_file.is_file()
    assert downloads_dir / "publications_by_author.csv" not in downloads_dir.iterdir()

    # Unzip publications.zip and check contents
    with zipfile.ZipFile(publications_file, "r") as zip_file:
        zip_file.extractall(downloads_dir)

    # Assert that publications.csv exists
    csv_file = downloads_dir / "publications.csv"
    assert csv_file.is_file()
    dictionary_file = downloads_dir / "publications_data_dictionary.csv"
    assert dictionary_file.is_file()

    # Check that 'true' not 't' exists for the first publication
    with open(csv_file, "r") as f:
        lines = f.readlines()
        assert len(lines) == 2  # one publication + header
        assert "false,true,true" in lines[1]

    with open(dictionary_file, "r") as f:
        dictionary = f.read()
        assert (
            'author_list_names,String,"Pipe-delimited list of all authors.'
            in dictionary
        )
        assert "title,String,Title of the publication" in dictionary


def test_no_downloads_dir(tmp_path, test_reports_session):
    with pytest.raises(Exception, match="downloads directory missing at"):
        publication.generate_download_files(tmp_path)


def test_limit_openalex_only(
    dataset_incremental,
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

    publication.export_publications()

    with test_reports_session.begin() as session:
        pubs = session.query(Publications).all()
        assert len(pubs) == 0


def test_check_harvest_complete_no_harvests(test_incremental_session):
    with pytest.raises(Exception, match="No harvest records found"):
        publication.check_harvest_complete()


def test_check_harvest_complete_harvest_finished(test_incremental_session):
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 1, 1, 0, 0, 0),
                finished_at=datetime.datetime(2026, 1, 1, 1, 0, 0),
            )
        )

    assert publication.check_harvest_complete() is True


def test_check_harvest_complete_harvest_not_finished(test_incremental_session):
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 1, 1, 0, 0, 0),
                finished_at=None,
            )
        )

    assert publication.check_harvest_complete() is False


def test_check_harvest_complete_uses_most_recent(test_incremental_session):
    """When there are multiple harvests, only the most recent one is checked."""
    with test_incremental_session.begin() as session:
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 1, 1, 0, 0, 0),
                finished_at=datetime.datetime(2026, 1, 1, 1, 0, 0),
            )
        )
        session.add(
            Harvest(
                created_at=datetime.datetime(2026, 2, 1, 0, 0, 0),
                finished_at=None,
            )
        )

    assert publication.check_harvest_complete() is False
