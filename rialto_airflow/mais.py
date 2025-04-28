import logging
from collections import defaultdict
from datetime import date
from typing import Any, Optional, Union
from rialto_airflow.utils import normalize_orcid

import requests
from requests_oauthlib import OAuth2Session

# Type aliases for clarity
ORCIDRecord = dict[str, Any]
ORCIDStats = list[Union[str, int, float]]

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TokenFetchError(LookupError):
    pass


def get_token(client_id: str, client_secret: str, base_url: str) -> str:
    """Retrieves an OAuth2 access token."""

    token_url = f"{base_url}/oauth2/token"
    data = {"grant_type": "client_credentials"}
    logger.info(f"Fetching token from {token_url}")

    try:
        response = requests.post(token_url, auth=(client_id, client_secret), data=data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        access_token = data.get("access_token")
        if access_token:
            logger.info("Successfully obtained access token.")  # Log success
            return access_token
        else:
            err_msg = f"No 'access_token' found in JSON response: {data}"
            logger.error(err_msg)  # Log specific error
            raise TokenFetchError(err_msg)

    except requests.exceptions.RequestException as e:  # Catch all requests exceptions
        logger.exception(f"Error during token request: {e}")
        if e.response is not None and hasattr(e.response, "text"):
            logger.error(f"Response content: {e.response.text}")
        raise TokenFetchError("Error during token request, see logs") from e


def get_response(
    access_token: str, url: str, params: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    """Retrieves a JSON response from the MAIS ORCID API."""
    oauth_session = OAuth2Session(
        token={"access_token": access_token, "token_type": "Bearer"}
    )
    response = oauth_session.get(url, params=params)
    response.raise_for_status()  # Raise HTTPError for bad responses
    return response.json()


def fetch_orcid_users(
    access_token: str, base_url: str, path: str, limit: Optional[int] = None
) -> list[ORCIDRecord]:
    """Fetches ORCID user records from the MAIS ORCID API, handling pagination."""

    orcid_users: list[ORCIDRecord] = []
    per_page = 100  # This max value from the API.
    url = f"{base_url}/orcid/v1{path}"
    params = {"page": 1, "per_page": per_page}

    while True:
        logger.info(f"Fetching page {params['page']} from {url}")
        data = get_response(access_token, url, params=params)

        results = data.get("results", [])
        orcid_users.extend(results)

        if limit and len(orcid_users) >= limit:
            break

        next_link = data.get("links", {}).get("next")
        if not next_link:
            break

        params["page"] += 1  # Increment for next page
        url = f"{base_url}/orcid/v1{next_link}"  # Construct url using next link value

    return orcid_users[:limit] if limit else orcid_users


def fetch_orcid_user(access_token: str, base_url: str, user_id: str) -> ORCIDRecord:
    """Fetches a single ORCID user record by ORCID iD or SUNet ID from the MAIS ORCID API."""
    cleaned_id = normalize_orcid(user_id)
    return get_response(access_token, f"{base_url}/orcid/v1/users/{cleaned_id}")


def current_orcid_users(access_token: str, base_url: str) -> list[ORCIDRecord]:
    """Retrieves the current ORCID records from the MAIS ORCID API."""

    if base_url is None:
        raise ValueError("AIRFLOW_VAR_MAIS_BASE_URL is a required value")

    all_users = fetch_orcid_users(access_token, base_url, "/users?scope=ANY")

    # Create a dictionary to store the most recent record for each ORCID iD
    current_users_by_orcid: dict[str, ORCIDRecord] = {}

    for user in all_users:
        if "orcid_id" in user:
            orcid_id = user["orcid_id"]
            current_users_by_orcid[orcid_id] = user  # Overwrite with the latest record

    return list(current_users_by_orcid.values())


def count_scopes(records: list[ORCIDRecord]) -> dict[str, int]:
    """Counts the occurrences of different scopes within the ORCID records."""

    value_counts: dict[Any, int] = defaultdict(int)

    for record in records:
        scopes = record.get("scope")
        if isinstance(scopes, str):
            value_counts[scopes] += 1
        elif isinstance(scopes, list):
            for scope in scopes:
                value_counts[scope] += 1

    return value_counts


def get_orcid_stats(current_records: list[ORCIDRecord]) -> ORCIDStats:
    """Calculates and returns ORCID user statistics."""

    today_str = date.today().strftime("%m/%d/%Y")
    scope_counts = count_scopes(current_records)

    read_limited_count = scope_counts.get("/read-limited", 0)
    read_write_count = scope_counts.get("/activities/update", 0)
    read_scope_count = (
        read_limited_count - read_write_count
    )  # Users with ONLY read access

    return [
        today_str,
        read_scope_count,
        read_write_count,
    ]
