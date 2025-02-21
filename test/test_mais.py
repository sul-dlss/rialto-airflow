import os
import pytest
import requests
from typing import Any, Dict, List, Union

from datetime import date
from rialto_airflow import mais

ORCIDRecord = Dict[str, Any]
ORCIDStats = List[Union[str, int, float]]

TOTAL_USERS_CONSTANT = 72000


@pytest.fixture(scope="module")
def base_url():
    return os.environ.get("AIRFLOW_VAR_MAIS_BASE_URL")


@pytest.fixture(scope="module")
def client_id():
    return os.environ.get("AIRFLOW_VAR_MAIS_CLIENT_ID")


@pytest.fixture(scope="module")
def client_secret():
    return os.environ.get("AIRFLOW_VAR_MAIS_SECRET")


@pytest.fixture(scope="module")
def access_token(client_id, client_secret, base_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    return mais.get_token(client_id, client_secret, base_url)


@pytest.mark.mais_tests
def test_get_token_success(base_url, client_id, client_secret):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    token = mais.get_token(client_id, client_secret, base_url)
    assert token is not None
    assert isinstance(token, str)
    assert len(token) > 0


@pytest.mark.mais_tests
def test_get_token_failure(base_url, client_id, client_secret):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    token = mais.get_token("dummy_invalid_id", "dummy_invalid_secret", base_url)
    assert token is None


@pytest.mark.mais_tests
def test_get_response_success(access_token, base_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    test_url = f"{base_url}/mais/orcid/v1/users"
    params = {"page": 1, "per_page": 1}
    try:
        response_data = mais.get_response(access_token, test_url, params=params)
        assert isinstance(response_data, dict)
        assert "results" in response_data
        assert isinstance(response_data["results"], list)
    except requests.exceptions.RequestException as e:
        pytest.fail(f"API request failed: {e}")


@pytest.mark.mais_tests
def test_fetch_orcid_users_with_limit(access_token, base_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    path = "/users?scope=ANY"
    limit = 5
    users = mais.fetch_orcid_users(access_token, base_url, path, limit=limit)
    assert isinstance(users, list)
    assert len(users) == limit


@pytest.mark.mais_tests
def test_fetch_orcid_users_invalid_path(access_token, base_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    invalid_path = "/invalid_path"
    with pytest.raises(requests.exceptions.HTTPError):
        mais.fetch_orcid_users(access_token, base_url, invalid_path)


@pytest.mark.mais_tests
def test_fetch_orcid_user_valid_id(access_token, base_url):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    orcid_id = "https://orcid.org/0009-0008-2120-5722"  # Example Valid ID
    user_data = mais.fetch_orcid_user(access_token, base_url, orcid_id)
    assert isinstance(user_data, dict)


@pytest.mark.mais_tests
def test_current_orcid_users(access_token):
    if not (client_secret and client_id):
        pytest.skip("No MAIS credentials available")
    current_users = mais.current_orcid_users(access_token)
    assert isinstance(current_users, list)
    assert len(current_users) > 0
    seen_orcids = set()
    for user in current_users:
        orcid_id = user.get("orcid_id")
        assert orcid_id is not None
        assert orcid_id not in seen_orcids
        seen_orcids.add(orcid_id)


def test_count_scopes_empty_list():
    records: List[ORCIDRecord] = []
    scope_counts = mais.count_scopes(records)
    assert scope_counts == {}


def test_count_scopes_string_scopes():
    records = [{"scope": "scope1"}, {"scope": "scope2"}, {"scope": "scope1"}]
    scope_counts = mais.count_scopes(records)
    assert scope_counts == {"scope1": 2, "scope2": 1}


def test_count_scopes_list_scopes():
    """Tests with list scopes."""
    records: List[ORCIDRecord] = [
        {"scope": ["scope1", "scope2"]},
        {"scope": ["scope2", "scope3"]},
        {"scope": "scope1"},  # Mixed string and list
    ]
    scope_counts = mais.count_scopes(records)
    assert scope_counts == {"scope1": 2, "scope2": 2, "scope3": 1}


def test_get_orcid_stats_with_data():
    current_records: List[ORCIDRecord] = [
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
    total_scoped = read_scope_count + read_write_count
    expected_percent_write_scope = (
        round((read_write_count / total_scoped), 2) if total_scoped else 0.0
    )
    today_str = date.today().strftime("%m/%d/%Y")

    assert stats == [
        today_str,
        TOTAL_USERS_CONSTANT - total_scoped,
        expected_percent_write_scope,
        read_scope_count,
        read_write_count,
        read_write_count,
        total_scoped,
    ]
