import os
import logging
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Union
from rialto_airflow.utils import normalize_orcid

import requests
from requests_oauthlib import OAuth2Session
import dotenv

# Load environment variables
dotenv.load_dotenv()

# Type aliases for clarity
ORCIDRecord = Dict[str, Any]
ORCIDStats = List[Union[str, int, float]]

TOTAL_USERS_CONSTANT = (
    72000  # Where does this come from? It's in the stats spreadsheet.
)

BASE_URL = os.environ.get("AIRFLOW_VAR_MAIS_BASE_URL")
CLIENT_ID = os.environ.get("AIRFLOW_VAR_MAIS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AIRFLOW_VAR_MAIS_SECRET")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_token(client_id: str, client_secret: str, base_url: str) -> Optional[str]:
    """Retrieves an OAuth2 access token."""

    token_url = f"{base_url}/api/oauth/token"
    auth = f"{client_id}:{client_secret}"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {auth}",
    }

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    try:
        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        access_token = data.get("access_token")
        if access_token:
            logger.info("Successfully obtained access token.")  # Log success
            return access_token
        else:
            logger.error(
                f"No 'access_token' found in JSON response: {data}"
            )  # Log specific error
            return None

    except requests.exceptions.RequestException as e:  # Catch all requests exceptions
        logger.exception(f"Error during token request: {e}")
        if e.response is not None and hasattr(e.response, "text"):
            logger.error(f"Response content: {e.response.text}")
        return None


def get_response(
    access_token: str, url: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Retrieves a JSON response from the MAIS ORCID API."""
    oauth_session = OAuth2Session(
        token={"access_token": access_token, "token_type": "Bearer"}
    )
    response = oauth_session.get(url, params=params)
    response.raise_for_status()  # Raise HTTPError for bad responses
    return response.json()


def fetch_orcid_users(
    access_token: str, base_url: str, path: str, limit: Optional[int] = None
) -> List[ORCIDRecord]:
    """Fetches ORCID user records from the MAIS ORCID API, handling pagination."""

    orcid_users: List[ORCIDRecord] = []
    per_page = 100  # This max value from the API.
    url = f"{base_url}/mais/orcid/v1{path}"
    params = {"page": 1, "per_page": per_page}

    while True:
        logger.info(f"Fetching page {params['page']}")
        data = get_response(access_token, url, params=params)

        results = data.get("results", [])
        orcid_users.extend(results)

        if limit and len(orcid_users) >= limit:
            break

        next_link = data.get("links", {}).get("next")
        if not next_link:
            break

        params["page"] += 1  # Increment for next page
        url = f"{base_url}/mais/orcid/v1{next_link}"  # Construct url using next link value

    return orcid_users[:limit] if limit else orcid_users


def fetch_orcid_user(access_token: str, base_url: str, user_id: str) -> ORCIDRecord:
    """Fetches a single ORCID user record by ORCID iD or SUNet ID from the MAIS ORCID API."""
    cleaned_id = normalize_orcid(user_id)
    return get_response(access_token, f"{base_url}/mais/orcid/v1/users/{cleaned_id}")


def current_orcid_users(access_token: str) -> List[ORCIDRecord]:
    """Retrieves the current ORCID records from the MAIS ORCID API."""

    all_users = fetch_orcid_users(access_token, BASE_URL, "/users?scope=ANY")

    # Create a dictionary to store the most recent record for each ORCID iD
    current_users_by_orcid: Dict[str, ORCIDRecord] = {}

    for user in all_users:
        if "orcid_id" in user:
            orcid_id = user["orcid_id"]
            current_users_by_orcid[orcid_id] = user  # Overwrite with the latest record

    return list(current_users_by_orcid.values())


def count_scopes(records: List[ORCIDRecord]) -> Dict[str, int]:
    """Counts the occurrences of different scopes within the ORCID records."""

    value_counts = defaultdict(int)

    for record in records:
        scopes = record.get("scope")
        if isinstance(scopes, str):
            value_counts[scopes] += 1
        elif isinstance(scopes, list):
            for scope in scopes:
                value_counts[scope] += 1

    return value_counts


def get_orcid_stats(current_records: List[ORCIDRecord]) -> ORCIDStats:
    """Calculates and returns ORCID user statistics."""

    today_str = date.today().strftime("%m/%d/%Y")
    scope_counts = count_scopes(current_records)

    read_limited_count = scope_counts.get("/read-limited", 0)
    read_write_count = scope_counts.get("/activities/update", 0)
    read_scope_count = (
        read_limited_count - read_write_count
    )  # Users with ONLY read access
    total_scoped = read_scope_count + read_write_count
    total_users = TOTAL_USERS_CONSTANT - total_scoped
    write_scope = read_limited_count - read_scope_count
    percent_write_scope = (
        round((read_write_count / total_scoped), 2) if total_scoped else 0.0
    )

    return [
        today_str,
        total_users,
        percent_write_scope,
        read_scope_count,
        read_write_count,
        write_scope,
        total_scoped,
    ]
