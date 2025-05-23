import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from rialto_airflow import database
from rialto_airflow.database import Author
from rialto_airflow.snapshot import Snapshot


@pytest.fixture
def snapshot(tmp_path):
    return Snapshot.create(tmp_path)


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
    snapshot,
    mock_rialto_postgres,
    monkeypatch,
    teardown_database,
):
    if database.database_exists(snapshot.database_name):
        database.drop_database(snapshot.database_name)

    try:
        database.create_database(snapshot.database_name)

        with null_pool_engine(snapshot.database_name).connect() as conn:
            # Verify that the database exists and a connection was able to be made
            assert conn
    finally:
        # even if exception raised, tear down the database
        teardown_database(snapshot.database_name)


def test_drop_database(
    snapshot,
    mock_rialto_postgres,
    monkeypatch,
    teardown_database,
):
    try:
        database.create_database(snapshot.database_name)

        with null_pool_engine(snapshot.database_name).connect() as conn:
            # Verify that the database exists and a connection was able to be made
            assert conn

        assert (database.database_exists(snapshot.database_name)) is True

        database.drop_database(snapshot.database_name)

        assert (database.database_exists(snapshot.database_name)) is False

    finally:
        # even if exception raised, tear down the database
        if database.database_exists(snapshot.database_name):
            teardown_database(snapshot.database_name)


def test_create_schema(
    snapshot,
    mock_rialto_postgres,
    monkeypatch,
    teardown_database,
):
    # During testing, we want to be able to drop the database at the end.
    # Mocking the engine obtained by create_schema to avoid connections staying open and preventing teardown.
    def mock_engine_setup(db_name):
        return null_pool_engine(db_name)

    monkeypatch.setattr(database, "engine_setup", mock_engine_setup)

    try:
        database.create_database(snapshot.database_name)
        database.create_schema(snapshot.database_name)
        # Verify that the tables exist and the columns match the schema
        engine = null_pool_engine(snapshot.database_name)
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
                "dim_json",
                "openalex_json",
                "sulpub_json",
                "wos_json",
                "pubmed_json",
                "created_at",
                "updated_at",
            }

            author_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='author'"
                )
            )
            author_columns = [row[0] for row in author_result]
            assert set(author_columns) == {
                "id",
                "sunet",
                "cap_profile_id",
                "orcid",
                "first_name",
                "last_name",
                "status",
                "academic_council",
                "primary_role",
                "schools",
                "departments",
                "primary_school",
                "primary_dept",
                "primary_division",
                "created_at",
                "updated_at",
            }

            association_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='pub_author_association'"
                )
            )
            association_columns = [row[0] for row in association_result]
            assert set(association_columns) == {"publication_id", "author_id"}

            funder_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='funder'"
                )
            )
            author_columns = [row[0] for row in funder_result]
            assert set(author_columns) == {
                "id",
                "name",
                "grid_id",
                "ror_id",
                "openalex_id",
                "federal",
                "created_at",
                "updated_at",
            }

            funder_association = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='pub_funder_association'"
                )
            )
            association_columns = [row[0] for row in funder_association]
            assert set(association_columns) == {"publication_id", "funder_id"}
    finally:
        # even if exception raised, tear down the database
        teardown_database(snapshot.database_name)


@pytest.fixture
def author(test_session):
    with test_session.begin() as session:
        session.add(
            database.Author(
                sunet="janes",
                cap_profile_id="12345",
                orcid="https://orcid.org/0000-0000-0000-0001",
                first_name="Jane",
                last_name="Stanford",
                status=True,
            )
        )


def test_author_fixture(test_session, author):
    with test_session.begin() as session:
        assert session.query(Author).where(Author.sunet == "janes").count() == 1
