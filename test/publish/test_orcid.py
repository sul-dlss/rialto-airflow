import csv
import dotenv
import pytest

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy_utils import create_database, database_exists, drop_database

from rialto_airflow.publish import orcid
from rialto_airflow import mais
from rialto_airflow.database import (
    engine_setup,
    RIALTO_REPORTS_DB_NAME,
)
from rialto_airflow.publish.reports_database import (
    create_schema,
    AuthorOrcids,
    OrcidIntegrationStats,
    ReportsSchemaBase,
)


dotenv.load_dotenv()


@pytest.fixture
def base_url():
    return "fake_mais_base_url"


@pytest.fixture
def token_url():
    return "fake_mais_token_url"


@pytest.fixture
def client_id():
    return "fake_mais_client_id"


@pytest.fixture
def client_secret():
    return "fake_mais_client_secret"


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def access_token(client_id, client_secret, token_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    return mais.get_token(client_id, client_secret, token_url)


@pytest.fixture
def authors_active_csv(tmp_path):
    # Create a fixture authors CSV file
    fixture_file = tmp_path / "authors_active.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "sunetid",
                "first_name",
                "last_name",
                "full_name",
                "orcidid",
                "orcid_update_scope",
                "cap_profile_id",
                "role",
                "academic_council",
                "primary_affiliation",
                "primary_school",
                "primary_department",
                "primary_division",
                "all_schools",
                "all_departments",
                "all_divisions",
                "active",
            ]
        )
        writer.writerow(
            [
                "janes",
                "Jane",
                "Stanford",
                "Jane Stanford",
                "https://orcid.org/0000-0000-0000-0001",
                "true",
                "12345",
                "staff",
                "false",
                "Engineering",
                "School of Engineering",
                "Computer Science",
                "Philanthropy Division",
                "Independent Labs, Institutes, and Centers (Dean of Research)|School of Humanities and Sciences",
                "Computer Science|Horticulture",
                "Philanthropy Division|Other Division",
                "true",
            ]
        )
    return fixture_file


@pytest.fixture(autouse=True)
def mock_current_orcid_users(monkeypatch, authors_active_csv):
    def _mocked(*args, **kwargs):
        # Return a list of dicts matching the CSV row
        return [
            {
                "sunetid": "janes",
                "first_name": "Jane",
                "last_name": "Stanford",
                "full_name": "Jane Stanford",
                "orcidid": "https://orcid.org/0000-0000-0000-0001",
                "orcid_update_scope": "true",
                "cap_profile_id": "12345",
                "role": "staff",
                "academic_council": "false",
                "primary_affiliation": "Engineering",
                "primary_school": "School of Engineering",
                "primary_department": "Computer Science",
                "primary_division": "Philanthropy Division",
                "all_schools": "Independent Labs, Institutes, and Centers (Dean of Research)|School of Humanities and Sciences",
                "all_departments": "Computer Science|Horticulture",
                "all_divisions": "Philanthropy Division|Other Division",
                "active": "true",
            }
        ]

    monkeypatch.setattr(mais, "current_orcid_users", _mocked)


@pytest.fixture
def test_reports_session():
    """
    Returns a sqlalchemy session for the test database.
    """
    try:
        yield sessionmaker(engine_setup(RIALTO_REPORTS_DB_NAME, echo=True))
    finally:
        close_all_sessions()


@pytest.fixture
def setup_teardown_reports_schema(monkeypatch):
    """
    This pytest fixture will ensure that the test reports database exists and has
    the database schema configured. If the database exists it will be dropped
    and readded.
    """
    db_host = "postgresql+psycopg2://airflow:airflow@localhost:5432"

    db_name = RIALTO_REPORTS_DB_NAME
    db_uri = f"{db_host}/{db_name}"

    if database_exists(db_uri):
        drop_database(db_uri)

    create_database(db_uri)

    # note: rialto_airflow.database.create_schema wants the database name not uri
    monkeypatch.setenv("AIRFLOW_VAR_RIALTO_POSTGRES", db_host)
    create_schema(db_name, ReportsSchemaBase)

    # it's handy seeing SQL statements in the log when testing
    engine_setup(db_name, echo=True)

    yield

    drop_database(db_uri)


@pytest.mark.usefixtures("setup_teardown_reports_schema")
def test_export_author_orcids(
    test_reports_session, authors_active_csv, caplog, data_dir
):
    result = orcid.export_author_orcids(data_dir)
    # Add assertions here to verify the expected behavior
    assert result is True

    with test_reports_session.begin() as session:
        rows = session.query(AuthorOrcids).all()
        assert len(rows) == 1
        assert rows[0].orcidid == "https://orcid.org/0000-0000-0000-0001"
        assert rows[0].orcid_update_scope # is True
        assert rows[0].sunetid == "janes"
        assert rows[0].full_name == "Jane Stanford"
        assert rows[0].role == "staff"
        assert rows[0].primary_affiliation == "Engineering"
        assert rows[0].primary_school == "School of Engineering"
        assert rows[0].primary_department == "Computer Science"
        assert rows[0].primary_division == "Philanthropy Division"

    assert "finished writing author_orcids table" in caplog.text


@pytest.mark.usefixtures("setup_teardown_reports_schema")
def test_export_orcid_integration_stats(
    monkeypatch,
    test_reports_session,
    caplog,
    client_id,
    client_secret,
    token_url,
    base_url,
):
    # Mock current_users
    def mock_current_orcid_users(client_id, client_secret, token_url, base_url):
        return [
            {
                "orcidid": "https://orcid.org/0000-0000-0000-0001",
                "sunetid": "janes",
                "full_name": "Jane Stanford",
            }
        ]

    monkeypatch.setattr(orcid, "current_orcid_users", mock_current_orcid_users)

    # Mock get_orcid_stats
    def mock_get_orcid_stats(users):
        return ["2026-09-01", 5, 3]

    monkeypatch.setattr(orcid, "get_orcid_stats", mock_get_orcid_stats)

    # Mock get_token
    def mock_token(client_id, client_secret, token_url):
        return "fake_access_token"

    monkeypatch.setattr(mais, "get_token", mock_token)

    stats = orcid.export_orcid_integration_stats(
        client_id, client_secret, token_url, base_url
    )
    assert stats == ["2026-09-01", 5, 3]

    # Check the database for the inserted row
    with test_reports_session.begin() as session:
        rows = session.query(OrcidIntegrationStats).all()
        assert len(rows) == 1
        assert rows[0].date_label == "2026-09-01"
        assert rows[0].read_only_scope == 5
        assert rows[0].read_write_scope == 3
