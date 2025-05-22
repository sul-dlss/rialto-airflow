import datetime
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from rialto_airflow.honeybadger import default_args

from rialto_airflow.mais import (
    current_orcid_users,
    get_orcid_stats,
)

import rialto_airflow.google as google
from rialto_airflow.utils import rialto_active_authors_file
from rialto_airflow.validate import validate_orcid_tableau, validation_report

data_dir = Path(Variable.get("data_dir"))
mais_base_url = Variable.get("mais_base_url")
mais_token_url = Variable.get("mais_token_url")
mais_client_id = Variable.get("mais_client_id")
mais_client_secret = Variable.get("mais_secret")
gcp_conn_id = Variable.get("google_connection")
google_drive_id = Variable.get(
    "google_drive_id", os.environ.get("AIRFLOW_TEST_GOOGLE_DRIVE_ID")
)


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def publish_orcid():
    @task
    def update_authors():
        """
        Update the authors_active.csv file in Google Drive with the latest active authors data CSV file.
        """
        active_authors_file = rialto_active_authors_file(data_dir)
        google_folder_id = google.get_file_id(google_drive_id, "orcid-dashboard")
        logging.info(
            f"Uploading {active_authors_file} to google drive folder id {google_folder_id}"
        )
        google.upload_or_replace_file_in_google_drive(
            active_authors_file,
            google_folder_id,
        )
        return True

    @task
    def update_orcid_integration_stats():
        """
        Get current ORCID integration stats from the ORCID integration API and write to file in Google Drive.
        """
        orcid_integration_sheet_id = google.get_file_id(
            google.get_file_id(google_drive_id, "orcid-dashboard"),
            "orcid-integration-stats",
        )
        current_users = current_orcid_users(
            mais_client_id, mais_client_secret, mais_token_url, mais_base_url
        )
        orcid_stats = get_orcid_stats(current_users)
        logging.info(f"Adding orcid stats row to {orcid_integration_sheet_id}")
        google.append_rows_to_google_sheet(
            orcid_integration_sheet_id,
            [orcid_stats],
            "integration-stats",
        )
        return orcid_stats

    @task
    def validate_tableau():
        """
        Run calculations on the published csv files and send a report so the numbers in the Tableau dashboard
        can be manually verified.
        """
        authors_file_id = google.get_file_id(
            google.get_file_id(google_drive_id, "orcid-dashboard"),
            "authors_active.csv",
        )
        orcid_integration_sheet_id = google.get_file_id(
            google.get_file_id(google_drive_id, "orcid-dashboard"),
            "orcid-integration-stats",
        )
        authors_df = google.read_csv_from_google_drive(authors_file_id)
        orcid_integration_sheet_df = google.read_csv_from_google_drive(
            orcid_integration_sheet_id
        )
        expected_calculations = validate_orcid_tableau(
            authors_df, orcid_integration_sheet_df
        )
        report = validation_report("ORCID Validation Report", expected_calculations)
        return report

    authors = update_authors()
    stats = update_orcid_integration_stats()
    validate = validate_tableau()

    (authors, stats) >> validate


publish_orcid()
