import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy_utils import create_database, database_exists, drop_database

from rialto_airflow.database import create_schema, engine_setup


@pytest.fixture
def test_engine(monkeypatch):
    """
    This pytest fixture will ensure that the rialto_test database exists and has
    the database schema configured. If the database exists it will be dropped
    and readded.
    """
    db_host = "postgresql+psycopg2://airflow:airflow@localhost:5432"
    monkeypatch.setenv("AIRFLOW_VAR_RIALTO_POSTGRES", db_host)

    db_name = "rialto_test"
    db_uri = f"{db_host}/{db_name}"

    if database_exists(db_uri):
        drop_database(db_uri)

    create_database(db_uri)

    # note: rialto_airflow.database.create_schema wants the database name not uri
    create_schema(db_name)

    return engine_setup(db_name)


@pytest.fixture
def test_conn(test_engine):
    """
    Returns a sqlalchemy connection for the test database.
    """
    return test_engine.connect()


@pytest.fixture
def test_session(test_engine):
    """
    Returns a sqlalchemy session for the test database.
    """
    try:
        yield sessionmaker(test_engine)
    finally:
        close_all_sessions()
