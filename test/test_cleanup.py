from datetime import datetime, timedelta
import logging

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


def test_cleanup_author_files(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    # set up some timestamps
    now = datetime.now()
    time_format = "%m-%d-%Y"
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
    p6 = tmp_path / "authors_active.csv.2025-05-03"  # wrong format, should be ignored

    # write some data to the files
    p0.open("w").write("test")
    p1.open("w").write("test")
    p2.open("w").write("test")
    p3.open("w").write("test")
    p4.open("w").write("test")
    p5.open("w").write("test")
    p6.open("w").write("bogus")

    # do the cleanup!
    cleanup_author_files(30, tmp_path)

    # the 40 day old files should be gone but the others are still there
    assert p0.is_file()
    assert p1.is_file()
    assert not p2.is_file()
    assert p3.is_file()
    assert p4.is_file()
    assert not p5.is_file()
    assert p6.is_file()  # wrong format, should be ignored without errors
    assert (
        "Skipping file authors_active.csv.2025-05-03 as it does not match the expected date format"
        in caplog.text
    )


def test_cleanup_snapshots_continues_on_drop_error(tmp_path, monkeypatch, caplog):
    # create snapshots folder with two old snapshot directories
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()

    s1 = snapshots / "20000101000000"
    s2 = snapshots / "20000202000000"
    s1.mkdir()
    s2.mkdir()
    (s1 / "file.txt").write_text("one")
    (s2 / "file.txt").write_text("two")

    # monkeypatch database helpers imported in cleanup module
    calls = []

    def fake_drop(name):
        calls.append(name)
        # fail the first drop, succeed on the second
        if len(calls) == 1:
            raise RuntimeError("boom")

    # cleanup now iterates database_names(); return the two rialto_ names in order
    monkeypatch.setattr(
        "rialto_airflow.cleanup.database_names",
        lambda: [f"rialto_{s1.name}", f"rialto_{s2.name}"],
    )
    monkeypatch.setattr("rialto_airflow.cleanup.drop_database", fake_drop)

    caplog.set_level(logging.ERROR)
    # run cleanup with interval 0 so folders are considered old
    cleanup_snapshots(0, str(tmp_path))

    # both snapshot folders should be removed by rmtree
    assert not s1.exists()
    assert not s2.exists()

    # drop_database should have been called for both databases (in the order returned by database_names)
    assert calls == [f"rialto_{s1.name}", f"rialto_{s2.name}"]

    # the failure from the first drop (for the first name) should have been logged
    assert any(
        f"Failed to drop database rialto_{s1.name}" in rec.getMessage()
        and "boom" in rec.getMessage()
        for rec in caplog.records
    )


def test_cleanup_ignores_non_timestamped_rialto_names(tmp_path, monkeypatch):
    """Ensure databases like `rialto-airflow` and `rialto_reports_data` are not dropped.

    The cleanup logic only drops databases matching `rialto_` followed by a 14-digit timestamp.
    """
    # prepare a snapshots dir so cleanup_snapshots can iterate folders
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()

    calls = []

    def fake_drop(name):
        calls.append(name)

    # Return a mix of names: two non-timestamped that should be ignored and one timestamped that should be dropped
    monkeypatch.setattr(
        "rialto_airflow.cleanup.database_names",
        lambda: ["rialto-airflow", "rialto_reports_data", "rialto_20000101000000"],
    )
    monkeypatch.setattr("rialto_airflow.cleanup.drop_database", fake_drop)

    # run cleanup with 0 interval so the timestamped db is considered old
    from rialto_airflow.cleanup import cleanup_snapshots

    cleanup_snapshots(0, str(tmp_path))

    # ensure the non-timestamped names were not dropped
    assert "rialto-airflow" not in calls
    assert "rialto_reports_data" not in calls
    # ensure postgres is not dropped!
    assert "postgres" not in calls
    # the timestamped name should have been passed to drop_database
    assert "rialto_20000101000000" in calls


def test_logs_when_rmtree_fails(tmp_path, monkeypatch, caplog):
    """Ensure that an exception during shutil.rmtree is logged and does not crash cleanup."""
    # create a snapshots directory and a single old snapshot folder
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()

    folder = snapshots / "20000101000000"
    folder.mkdir()
    (folder / "file.txt").write_text("x")

    # monkeypatch rmtree in the cleanup module to raise
    def fake_rmtree(path):
        raise RuntimeError("rmtree boom")

    monkeypatch.setattr("rialto_airflow.cleanup.shutil.rmtree", fake_rmtree)
    # avoid touching the database layer during this test
    monkeypatch.setattr("rialto_airflow.cleanup.database_names", lambda: [])

    caplog.set_level(logging.ERROR)

    # run cleanup; use interval 0 so the folder is considered old
    from rialto_airflow.cleanup import cleanup_snapshots

    cleanup_snapshots(0, str(tmp_path))

    # The failing rmtree should have been logged with exception
    assert any(
        "Failed to delete folder" in rec.getMessage() and "boom" in rec.getMessage()
        for rec in caplog.records
    )


def test_cleanup_drops_timestamped_database(tmp_path, monkeypatch):
    """Ensure cleanup_snapshots calls drop_database for timestamped rialto databases."""
    # create snapshots dir so cleanup iterates without error
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()

    calls = []

    def fake_drop(name):
        calls.append(name)

    # return a single timestamped database name that should be dropped
    monkeypatch.setattr(
        "rialto_airflow.cleanup.database_names",
        lambda: ["rialto_20000101000000"],
    )
    monkeypatch.setattr("rialto_airflow.cleanup.drop_database", fake_drop)

    from rialto_airflow.cleanup import cleanup_snapshots

    cleanup_snapshots(0, str(tmp_path))

    assert calls == ["rialto_20000101000000"]
