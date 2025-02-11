import csv
from pathlib import Path

import os
import pytest

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from rialto_airflow import utils


@pytest.fixture
def authors_csv(tmp_path):
    # Create a fixture authors CSV file
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sunetid", "orcidid"])
        writer.writerow(["author1", "https://orcid.org/0000-0000-0000-0001"])
        writer.writerow(["author2", ""])
        writer.writerow(["author3", "https://orcid.org/0000-0000-0000-0002"])
    return fixture_file


def test_create_snapshot_dir(tmp_path):
    snap_dir = Path(utils.create_snapshot_dir(tmp_path))
    assert snap_dir.is_dir()


@pytest.fixture
def mock_rialto_postgres(monkeypatch):
    # Set up the environment variable for the PostgreSQL connection
    monkeypatch.setenv(
        "AIRFLOW_VAR_RIALTO_POSTGRES",
        "postgresql+psycopg2://airflow:airflow@localhost:5432",
    )


@pytest.fixture
def teardown_database():
    def perform_teardown_database(db_name):
        # Clean up by creating a new engine and connection and then drop the database
        teardown_conn = f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/postgres"
        teardown_engine = create_engine(teardown_conn, poolclass=NullPool)
        with teardown_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(text(f"drop database {db_name}"))
            connection.close()

    return perform_teardown_database


def test_create_database(tmp_path, mock_rialto_postgres, teardown_database):
    db_name = utils.create_database(tmp_path)
    assert db_name == "rialto_" + Path(tmp_path).name

    # Using NullPool to avoid connections to the database staying open and preventing later cleanup
    postgres_conn = f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/{db_name}"
    engine = create_engine(postgres_conn, poolclass=NullPool)
    conn = engine.connect()
    try:
        # Verify that the database exists and a connection was able to be made
        assert conn
        conn.close()
    finally:
        teardown_database(db_name)


def test_rialto_authors_orcids(tmp_path, authors_csv):
    orcids = utils.rialto_authors_orcids(authors_csv)
    assert len(orcids) == 2
    assert "https://orcid.org/0000-0000-0000-0001" in orcids


def test_rialto_authors_file():
    csv_file = utils.rialto_authors_file("test/data")
    assert Path(csv_file).is_file()

    with pytest.raises(Exception):
        utils.rialto_authors_file("/no/authors/file/here")


def test_invert_dict():
    dict = {
        "person_id1": ["pub_id1", "pub_id2", "pub_id3"],
        "person_id2": ["pub_id2", "pub_id4", "pub_id5"],
        "person_id3": ["pub_id5", "pub_id6", "pub_id7"],
    }

    inverted_dict = utils.invert_dict(dict)
    assert len(inverted_dict.items()) == 7
    assert sorted(inverted_dict.keys()) == [
        "pub_id1",
        "pub_id2",
        "pub_id3",
        "pub_id4",
        "pub_id5",
        "pub_id6",
        "pub_id7",
    ]
    assert inverted_dict["pub_id2"] == ["person_id1", "person_id2"]


def test_normalize_doi():
    assert utils.normalize_doi("https://doi.org/10.1234/5678") == "10.1234/5678"
    assert utils.normalize_doi("https://dx.doi.org/10.1234/5678") == "10.1234/5678"
    assert (
        utils.normalize_doi("10.1103/PhysRevLett.96.07390")
        == "10.1103/physrevlett.96.07390"
    )
    assert utils.normalize_doi(" doi: 10.1234/5678 ") == "10.1234/5678"
