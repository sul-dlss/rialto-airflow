import csv
import dotenv
import pytest


from rialto_airflow import mais
from rialto_airflow.publish import orcid
from rialto_airflow.schema.reports import (
    AuthorOrcids,
    OrcidIntegrationStats,
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
        writer.writerow(
            [
                "senaj",
                "Enaj",
                "Stanford",
                "Enaj Stanford",
                None,
                "false",
                "23456",
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


@pytest.fixture
def rialto_reports_db_name(monkeypatch):
    monkeypatch.setattr(orcid, "RIALTO_REPORTS_DB_NAME", "rialto_reports_test")


def test_export_author_orcids(
    test_reports_session, rialto_reports_db_name, authors_active_csv, caplog, data_dir
):
    result = orcid.export_author_orcids(data_dir)
    assert result is True

    with test_reports_session.begin() as session:
        rows = session.query(AuthorOrcids).order_by(AuthorOrcids.sunetid).all()
        assert len(rows) == 2
        assert rows[0].orcidid == "https://orcid.org/0000-0000-0000-0001"
        assert rows[0].orcid_update_scope  # is True
        assert rows[0].sunetid == "janes"
        assert rows[0].full_name == "Jane Stanford"
        assert rows[0].role == "staff"
        assert rows[0].primary_affiliation == "Engineering"
        assert rows[0].primary_school == "School of Engineering"
        assert rows[0].primary_department == "Computer Science"
        assert rows[0].primary_division == "Philanthropy Division"
        assert rows[1].sunetid == "senaj"
        assert rows[1].orcidid is None

    assert "finished writing author_orcids table" in caplog.text


@pytest.fixture
def mock_mais_token(monkeypatch):
    def mock_token(client_id, client_secret, token_url):
        return "fake_access_token"

    monkeypatch.setattr(mais, "get_token", mock_token)


@pytest.fixture
def mock_current_orcid_users(
    monkeypatch, client_id, client_secret, token_url, base_url
):
    def mock_orcid_users(client_id, client_secret, token_url, base_url):
        return [
            {
                "orcidid": "https://orcid.org/0000-0000-0000-0001",
                "sunetid": "janes",
                "full_name": "Jane Stanford",
            }
        ]

    monkeypatch.setattr(orcid, "current_orcid_users", mock_orcid_users)


@pytest.fixture
def mock_get_orcid_stats(monkeypatch):
    # Avoid contacting live MAIS API for data
    def mock_orcid_stats(users):
        return ["09/01/2025", 5, 3]

    monkeypatch.setattr(orcid, "get_orcid_stats", mock_orcid_stats)


def test_export_orcid_integration_stats(
    monkeypatch,
    test_reports_session,
    rialto_reports_db_name,
    mock_mais_token,
    mock_current_orcid_users,
    mock_get_orcid_stats,
    caplog,
    client_id,
    client_secret,
    token_url,
    base_url,
):
    stats = orcid.export_orcid_integration_stats(
        client_id, client_secret, token_url, base_url
    )
    assert stats == ["09/01/2025", 5, 3]
    assert (
        "wrote {'date_label': '09/01/2025', 'read_only_scope': 5, 'read_write_scope': 3} to orcid_integration_stats table"
        in caplog.text
    )

    # Check the database for the inserted row with new stats
    with test_reports_session.begin() as session:
        assert session.query(OrcidIntegrationStats).count() == 1
        row = (
            session.query(OrcidIntegrationStats)
            .where(OrcidIntegrationStats.date_label == "09/01/2025")
            .scalar()
        )
        assert row is not None
        assert row.date_label == "09/01/2025"
        assert row.read_only_scope == 5
        assert row.read_write_scope == 3
