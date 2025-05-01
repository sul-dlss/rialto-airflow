from datetime import datetime, timedelta

from rialto_airflow.cleanup import cleanup_snapshots, cleanup_author_files
from rialto_airflow.database import create_database, database_exists


def test_cleanup_snapshots(tmp_path, monkeypatch):
    # since we aren't using the test_session from conftest we have to set the
    # environment variable for what database to talk to
    monkeypatch.setenv(
        "AIRFLOW_VAR_RIALTO_POSTGRES",
        "postgresql+psycopg2://airflow:airflow@localhost:5432",
    )

    # set up some timestamps
    now = datetime.now()
    time_format = "%Y%m%d%H%M%S"
    t0 = now.strftime(time_format)
    t1 = (now - timedelta(days=1)).strftime(time_format)
    t2 = (now - timedelta(days=40)).strftime(time_format)

    # set up some snapshot directories
    p0 = tmp_path / "snapshots" / t0
    p1 = tmp_path / "snapshots" / t1
    p2 = tmp_path / "snapshots" / t2

    # create the directories
    p0.mkdir(parents=True)
    p1.mkdir(parents=True)
    p2.mkdir(parents=True)

    # write a file to each directory
    (p0 / "pubs.csv").open("w").write("a,b")
    (p1 / "pubs.csv").open("w").write("a,b")
    (p2 / "pubs.csv").open("w").write("a,b")

    # create the databases
    create_database(f"rialto_{t0}")
    create_database(f"rialto_{t1}")
    create_database(f"rialto_{t2}")

    # cleanup snapshots that are older than 30 days
    cleanup_snapshots(30, tmp_path)

    # ensure that the snapshots are still there except for the 40 day old one

    assert database_exists(f"rialto_{t0}"), "database is still there"
    assert p0.is_dir(), "snapshot dir is still there"
    assert len(list(p0.iterdir())) == 1, "snapshot file is still there"

    assert database_exists(f"rialto_{t1}"), "database is still there"
    assert p1.is_dir(), "snapshot dir is still there"
    assert len(list(p1.iterdir())) == 1, "snapshot file is still there"

    assert not database_exists(f"rialto_{t2}"), "database is gone"
    assert not p2.is_dir(), "snapshot directory is gone"


def test_cleanup_author_files(tmp_path):
    # set up some timestamps
    now = datetime.now()
    time_format = "%Y-%m-%d"
    t1 = (now - timedelta(days=1)).strftime(time_format)
    t2 = (now - timedelta(days=40)).strftime(time_format)

    # set up some author paths
    p0 = tmp_path / "authors.csv"  # the current one
    p1 = tmp_path / f"authors.csv.{t1}"  # the recent one
    p2 = tmp_path / f"authors.csv.{t2}"  # the far past

    # set up some author paths
    p3 = tmp_path / "authors_active.csv"  # the current one
    p4 = tmp_path / f"authors_active.csv.{t1}"  # the recent one
    p5 = tmp_path / f"authors_active.csv.{t2}"  # the far past

    # write some data to the files
    p0.open("w").write("test")
    p1.open("w").write("test")
    p2.open("w").write("test")
    p3.open("w").write("test")
    p4.open("w").write("test")
    p5.open("w").write("test")

    # do the cleanup!
    cleanup_author_files(30, tmp_path)

    # the 40 day old files should be gone but the others are still there
    assert p0.is_file()
    assert p1.is_file()
    assert not p2.is_file()
    assert p3.is_file()
    assert p4.is_file()
    assert not p5.is_file()
