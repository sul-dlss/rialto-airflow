import dotenv
import os
import pytest
import requests
from typing import Any, Union

from datetime import date
from rialto_airflow import mais

ORCIDRecord = dict[str, Any]
ORCIDStats = list[Union[str, int, float]]

dotenv.load_dotenv()


@pytest.fixture(scope="module")
def base_url():
    return os.environ.get("AIRFLOW_VAR_MAIS_BASE_URL")


@pytest.fixture(scope="module")
def token_url():
    return os.environ.get("AIRFLOW_VAR_MAIS_TOKEN_URL")


@pytest.fixture(scope="module")
def client_id():
    return os.environ.get("AIRFLOW_VAR_MAIS_CLIENT_ID")


@pytest.fixture(scope="module")
def client_secret():
    return os.environ.get("AIRFLOW_VAR_MAIS_SECRET")


@pytest.fixture(scope="module")
def access_token(client_id, client_secret, token_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    return mais.get_token(client_id, client_secret, token_url)


def test_get_token_success(token_url, client_id, client_secret):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    token = mais.get_token(client_id, client_secret, token_url)
    assert token is not None
    assert isinstance(token, str)
    assert len(token) > 0


def test_get_token_failure(token_url, client_id, client_secret):
    # It's true, this test specifically doesn't use a valid client_id or client_secret, but
    # the presence of those values indicates we're able to reach the service in question (which
    # is not possible from e.g. CI).
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    with pytest.raises(mais.TokenFetchError):
        mais.get_token("dummy_invalid_id", "dummy_invalid_secret", token_url)


def test_get_response_success(access_token, base_url, client_id, client_secret):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    test_url = f"{base_url}/orcid/v1/users?scope=ANY&page_number=1&page_size=100"
    try:
        response_data = mais.get_response(access_token, test_url)
        assert isinstance(response_data, dict)
        assert "results" in response_data
        assert isinstance(response_data["results"], list)
    except requests.exceptions.RequestException as e:
        pytest.fail(f"API request failed: {e}")


def test_fetch_orcid_users_with_limit(client_id, client_secret, token_url, base_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    limit = 5
    users = mais.fetch_orcid_users(
        client_id, client_secret, token_url, base_url, limit=limit
    )
    assert isinstance(users, list)
    assert len(users) == limit


def test_fetch_orcid_user_valid_id(access_token, base_url, client_id, client_secret):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    orcid_id = "https://sandbox.orcid.org/0000-0002-4589-7232"  # Example Valid ID
    user_data = mais.fetch_orcid_user(access_token, base_url, orcid_id)
    assert isinstance(user_data, dict)


def test_current_orcid_users(
    client_id, client_secret, token_url, base_url, monkeypatch
):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    monkeypatch.setattr(
        mais, "page_size", lambda *args: 5
    )  # monkey patch a smaller page size so we can hit the paging code in UAT
    current_users = mais.current_orcid_users(
        client_id, client_secret, token_url, base_url
    )
    assert isinstance(current_users, list)
    assert len(current_users) > 0
    seen_orcids = set()
    for user in current_users:
        orcid_id = user.get("orcid_id")
        assert orcid_id is not None
        assert orcid_id not in seen_orcids
        seen_orcids.add(orcid_id)


def test_invalid_token_retry(
    client_id, client_secret, token_url, base_url, monkeypatch
):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    monkeypatch.setattr(
        mais, "get_token", lambda *args: "invalid_token"
    )  # monkey patch an invalid token
    with pytest.raises(requests.exceptions.HTTPError):
        mais.current_orcid_users(client_id, client_secret, token_url, base_url)


def test_count_scopes_empty_list():
    records: list[ORCIDRecord] = []
    scope_counts = mais.count_scopes(records)
    assert scope_counts == {}


def test_count_scopes_string_scopes():
    records = [{"scope": "scope1"}, {"scope": "scope2"}, {"scope": "scope1"}]
    scope_counts = mais.count_scopes(records)
    assert scope_counts == {"scope1": 2, "scope2": 1}


def test_count_scopes_list_scopes():
    """Tests with list scopes."""
    records: list[ORCIDRecord] = [
        {"scope": ["scope1", "scope2"]},
        {"scope": ["scope2", "scope3"]},
        {"scope": "scope1"},  # Mixed string and list
    ]
    scope_counts = mais.count_scopes(records)
    assert scope_counts == {"scope1": 2, "scope2": 2, "scope3": 1}


def test_get_orcid_stats_with_data():
    current_records: list[ORCIDRecord] = [
        {"scope": "/read-limited"},
        {"scope": ["/read-limited", "/activities/update"]},
        {"scope": "/activities/update"},
    ]

    stats = mais.get_orcid_stats(current_records)

    # Calculate expected values based on the test
    read_limited_count = (
        2  # Explicit /read-limited + the one implied by the last record
    )
    read_write_count = 2  # Two with /activities/update, implying /read-limited as well
    read_scope_count = (
        read_limited_count - read_write_count
    )  # Read without write access
    today_str = date.today().strftime("%m/%d/%Y")

    assert stats == [
        today_str,
        read_scope_count,
        read_write_count,
    ]
