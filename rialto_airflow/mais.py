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


def get_response(access_token: str, url: str) -> dict[str, Any]:
    """Retrieves a JSON response from the MAIS ORCID API."""
    oauth_session = OAuth2Session(
        token={"access_token": access_token, "token_type": "Bearer"}
    )
    response = oauth_session.get(url)
    response.raise_for_status()  # Raise HTTPError for bad responses
    return response.json()


def page_size() -> int:
    """Returns the page size for the API request."""
    return 100  # This is the max value from the API.


def fetch_orcid_users(
    mais_client_id: str,
    mais_client_secret: str,
    mais_token_url: str,
    base_url: str,
    limit: Optional[int] = None,
) -> list[ORCIDRecord]:
    """Fetches ORCID user records from the MAIS ORCID API, handling pagination."""

    orcid_users: list[ORCIDRecord] = []
    url = f"{base_url}/orcid/v1/users?scope=ANY&page_number=1&page_size={page_size()}"  # first page url
    access_token = get_token(mais_client_id, mais_client_secret, mais_token_url)

    while True:  # paging
        logger.info(f"Fetching {url}")
        tries = 0
        while True:  # retry loop for token expiration
            try:
                # Try to get the response
                data = get_response(access_token, url)
                break  # break out of the retry loop if we get a successful response
            except requests.exceptions.HTTPError as e:
                # if we get a 401 unathorized error from the API call, try to get a new token
                if e.response.status_code == 401:
                    tries += 1
                    logger.warning(
                        f"Attempt {tries}: Access token expired. Retrying..."
                    )
                    access_token = get_token(
                        mais_client_id, mais_client_secret, mais_token_url
                    )
                    if tries >= 3:
                        logger.error(
                            "Failed to fetch new access token after multiple attempts."
                        )
                        raise e  # break out of the retry loop if we fail three times to get a new token
                    continue  # Retry the current request with the new token
                else:
                    raise e  # Re-raise the exception if it's not a 401 error

        results = data.get("results", [])
        orcid_users.extend(results)

        if limit and len(orcid_users) >= limit:
            break

        self_link = data.get("links", {}).get("self")
        last_link = data.get("links", {}).get("last")
        # if we have reached the last page, the self and last links will be the same
        if self_link == last_link:
            break

        next_link = data.get("links", {}).get("next")
        logger.info(f"Self: {self_link}, Last: {last_link}, Next: {next_link}")

        url = f"{base_url}/orcid/v1{next_link}"  # Construct url using next link value

    return orcid_users[:limit] if limit else orcid_users


def fetch_orcid_user(access_token: str, base_url: str, user_id: str) -> ORCIDRecord:
    """Fetches a single ORCID user record by ORCID iD or SUNet ID from the MAIS ORCID API."""
    cleaned_id = normalize_orcid(user_id)
    return get_response(access_token, f"{base_url}/orcid/v1/users/{cleaned_id}")


def current_orcid_users(
    mais_client_id: str, mais_client_secret: str, mais_token_url: str, base_url: str
) -> list[ORCIDRecord]:
    """Retrieves the current ORCID records from the MAIS ORCID API."""

    if base_url is None:
        raise ValueError("AIRFLOW_VAR_MAIS_BASE_URL is a required value")

    all_users = fetch_orcid_users(
        mais_client_id, mais_client_secret, mais_token_url, base_url
    )

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
