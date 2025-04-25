import datetime
import logging

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.mais import (
    get_token,
    current_orcid_users,
    get_orcid_stats,
)

import rialto_airflow.google as google

from rialto_airflow.utils import rialto_active_authors_file

data_dir = Variable.get("data_dir")
mais_base_url = Variable.get("mais_base_url")
mais_client_id = Variable.get("mais_client_id")
mais_client_secret = Variable.get("mais_secret")
gcp_conn_id = Variable.get("google_connection")


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def publish_orcid():
    @task
    def update_authors():
        """
        Update the authors.csv file in Google Drive with the latest authors data CSV file.
        """
        authors_file = rialto_active_authors_file(data_dir)
        google_folder_id = google.orcid_dashboard_folder_id()
        logging.info(
            f"Uploading {authors_file} to google drive folder id {google_folder_id}"
        )
        google.upload_or_replace_file_in_google_drive(
            authors_file,
            google_folder_id,
        )
        return True

    @task
    def update_orcid_integration_stats():
        """
        Get current ORCID integration stats from the ORCID integration API and write to file in Google Drive.
        """
        orcid_integration_sheet_id = google.get_file_id(
            google.orcid_dashboard_folder_id(), "orcid-integration-stats"
        )
        access_token = get_token(mais_client_id, mais_client_secret, mais_base_url)
        current_users = current_orcid_users(access_token)
        orcid_stats = get_orcid_stats(current_users)
        logging.info(f"Adding orcid stats row to {orcid_integration_sheet_id}")
        google.append_rows_to_google_sheet(
            orcid_integration_sheet_id,
            [orcid_stats],
            "integration-stats",
        )
        return orcid_stats

    update_authors()

    update_orcid_integration_stats()


publish_orcid()
