import pytest
from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy_utils import create_database, database_exists, drop_database

from rialto_airflow.database import create_schema, engine_setup
from rialto_airflow.schema.harvest import (
    Author,
    Publication,
    pub_author_association,
)
from rialto_airflow.schema.harvest import HarvestSchemaBase
from rialto_airflow.snapshot import Snapshot


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
    create_schema(db_name, HarvestSchemaBase)

    # it's handy seeing SQL statements in the log when testing
    return engine_setup(db_name, echo=True)


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


@pytest.fixture
def mock_authors(test_session):
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Author(
                    sunet="janes",
                    cap_profile_id="12345",
                    orcid="https://orcid.org/0000-0000-0000-0001",
                    first_name="Jane",
                    last_name="Stanford",
                    status=True,
                ),
                Author(
                    sunet="lelands",
                    cap_profile_id="123456",
                    orcid="https://orcid.org/0000-0000-0000-0002",
                    first_name="Leland",
                    last_name="Stanford",
                    status=True,
                ),
                # this user intentionally lacks an ORCID to help test code that
                # ignores harvesting for users that lack an ORCID
                Author(
                    sunet="lelelandjr",
                    cap_profile_id="1234567",
                    orcid=None,
                    first_name="Leland",
                    last_name="Stanford",
                    status=True,
                ),
            ]
        )


@pytest.fixture
def mock_publication(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            sulpub_json={"sulpub": "data"},
            wos_json={"wos": "data"},
            dim_json={"dimensions": "data"},
        )
        session.add(pub)
        return pub


@pytest.fixture
def mock_association(test_session, mock_publication, mock_authors):
    with test_session.begin() as session:
        session.execute(
            insert(pub_author_association).values(
                # TODO: should the IDs be looked up in case they aren't always 1?
                publication_id=1,
                author_id=1,
            )
        )


@pytest.fixture
def snapshot(tmp_path):
    return Snapshot.create(data_dir=tmp_path, database_name="rialto_test")
