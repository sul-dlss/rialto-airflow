import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from rialto_airflow import database


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
    db_name = database.create_database(tmp_path)
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
