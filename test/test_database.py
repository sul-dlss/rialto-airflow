import datetime
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from rialto_airflow import database
from rialto_airflow.schema import rialto
from rialto_airflow.schema.rialto import RialtoSchemaBase


@pytest.fixture
def mock_rialto_postgres(monkeypatch):
    # Set up the environment variable for the PostgreSQL connection
    monkeypatch.setenv(
        "AIRFLOW_VAR_RIALTO_POSTGRES",
        "postgresql+psycopg2://airflow:airflow@localhost:5432",
    )


@pytest.fixture
def teardown_database():
    def perform_teardown_database(database_name):
        """Clean up by creating a new engine and connection and then drop the database"""
        teardown_engine = null_pool_engine("postgres")
        with teardown_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(text(f"drop database {database_name}"))

    return perform_teardown_database


def null_pool_engine(database_name):
    # engine with NullPool to avoid connections staying open and preventing later cleanup
    engine = create_engine(
        f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/{database_name}",
        poolclass=NullPool,
    )
    return engine


def test_create_database(
    mock_rialto_postgres,
    monkeypatch,
    teardown_database,
):
    db_name = "test_abc"

    try:
        database.create_database(db_name)

        with null_pool_engine(db_name).connect() as conn:
            # Verify that the database exists and a connection was able to be made
            assert conn
    finally:
        # even if exception raised, tear down the database
        teardown_database(db_name)


def test_drop_database(
    mock_rialto_postgres,
    monkeypatch,
    teardown_database,
):
    db_name = "test_123"

    try:
        database.create_database(db_name)

        with null_pool_engine(db_name).connect() as conn:
            # Verify that the database exists and a connection was able to be made
            assert conn

        assert (database.database_exists(db_name)) is True

        database.drop_database(db_name)

        assert (database.database_exists(db_name)) is False

    finally:
        # even if exception raised, tear down the database
        if database.database_exists(db_name):
            teardown_database(db_name)


def test_create_rialto_schema(mock_rialto_postgres, monkeypatch, teardown_database):
    database_name = rialto.RIALTO_DB_NAME

    # During testing, we want to be able to drop the database at the end.
    # Mocking the engine obtained by create_schema to avoid connections staying open and preventing teardown.
    def mock_engine_setup(db_name):
        return null_pool_engine(db_name)

    monkeypatch.setattr(database, "engine_setup", mock_engine_setup)

    try:
        if database.database_exists(database_name):
            database.drop_database(database_name)

        database.create_database(database_name)
        database.create_schema(database_name, RialtoSchemaBase)
        engine = null_pool_engine(database_name)
        with engine.connect() as conn:
            pub_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='publication'"
                )
            )
            pub_columns = [row[0] for row in pub_result]
            assert set(pub_columns) == {
                "id",
                "doi",
                "title",
                "pub_year",
                "open_access",
                "apc",
                "crossref_json",
                "dim_json",
                "openalex_json",
                "sulpub_json",
                "wos_json",
                "pubmed_json",
                "wos_id",
                "pubmed_id",
                "openalex_harvested",
                "dim_harvested",
                "sulpub_harvested",
                "wos_harvested",
                "pubmed_harvested",
                "distilled_at",
                "created_at",
                "updated_at",
                "types",
                "publisher",
                "journal_name",
                "academic_council_authored",
                "faculty_authored",
            }
    finally:
        # even if exception raised, tear down the database
        if database.database_exists(database_name):
            teardown_database(database_name)


def test_publication_last_harvested():
    pub = rialto.Publication()
    assert pub.last_harvested() is None

    now = datetime.datetime.now(datetime.timezone.utc)
    pub.openalex_harvested = now
    assert pub.last_harvested() == now

    earlier = now - datetime.timedelta(days=1)
    pub.dim_harvested = earlier
    assert pub.last_harvested() == now

    later = now + datetime.timedelta(days=1)
    pub.wos_harvested = later
    assert pub.last_harvested() == later


def test_publication_needs_distillation():
    pub = rialto.Publication()
    assert pub.needs_distillation() is True

    now = datetime.datetime.now(datetime.timezone.utc)
    pub.distilled_at = now
    pub.updated_at = now
    assert pub.needs_distillation() is False

    later = now + datetime.timedelta(seconds=1)
    pub.updated_at = later
    assert pub.needs_distillation() is True


def test_database_names(mock_rialto_postgres, teardown_database):
    # create two unique databases and ensure database_names() returns them
    name1 = f"rialto_test_{uuid.uuid4().hex[:8]}"
    name2 = f"rialto_test_{uuid.uuid4().hex[:8]}"

    try:
        database.create_database(name1)
        database.create_database(name2)

        # database.database_names() returns a list of names, but excludes airflow and postgres databases
        names = database.database_names()

        assert name1 in names
        assert name2 in names
        assert "rialto-airflow" not in names
        assert "postgres" not in names

    finally:
        # tear down both databases if they exist
        if database.database_exists(name1):
            teardown_database(name1)
        if database.database_exists(name2):
            teardown_database(name2)
