import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from rialto_airflow import database
from rialto_airflow.database import Author


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
        """Clean up by creating a new engine and connection and then drop the database"""
        teardown_engine = null_pool_engine("postgres")
        with teardown_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(text(f"drop database {db_name}"))

    return perform_teardown_database


def null_pool_engine(database_name):
    # engine with NullPool to avoid connections staying open and preventing later cleanup
    engine = create_engine(
        f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/{database_name}",
        poolclass=NullPool,
    )
    return engine


def test_create_database(tmp_path, mock_rialto_postgres, teardown_database):
    try:
        db_name = database.create_database(tmp_path)
        assert db_name == "rialto_" + Path(tmp_path).name

        with null_pool_engine(db_name).connect() as conn:
            # Verify that the database exists and a connection was able to be made
            assert conn
    finally:
        # even if exception raised, tear down the database
        teardown_database(db_name)


def test_create_schema(tmp_path, mock_rialto_postgres, monkeypatch, teardown_database):
    # During testing, we want to be able to drop the database at the end.
    # Mocking the engine obtained by create_schema to avoid connections staying open and preventing teardown.
    def mock_engine_setup(db_name):
        return null_pool_engine(db_name)

    monkeypatch.setattr(database, "engine_setup", mock_engine_setup)

    db_name = database.create_database(tmp_path)
    try:
        database.create_schema(db_name)
        # Verify that the tables exist and the columns match the schema
        engine = null_pool_engine(db_name)
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
    finally:
        # even if exception raised, tear down the database
        teardown_database(db_name)


@pytest.fixture
def author(test_session):
    with test_session.begin() as session:
        session.add(
            database.Author(
                sunet="mjgiarlo",
                orcid="0000-0002-2100-6108",
                first_name="Mike",
                last_name="Giarlo",
                status="active",
            )
        )


def test_author_fixture(test_session, author):
    with test_session.begin() as session:
        assert session.query(Author).where(Author.sunet == "mjgiarlo").count() == 1
