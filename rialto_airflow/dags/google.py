# An example DAG showing how to interact with Google Sheets and Google Drive using Airflow.
# Pull these examples into your own DAGs as needed

import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.suite.transfers.local_to_drive import (
    LocalFilesystemToGoogleDriveOperator,
)
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

gcp_conn_id = Variable.get("google_connection")
google_drive_id = Variable.get("google_drive_id")
google_sheet_id = Variable.get(
    "orcid_stats_sheet_id"
)  # setup in Airflow Admin -> Variables OR via vault/puppet environment variables


@dag(
    # schedule=@weekly,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def google():
    @task
    def clear_google_sheet(spreadsheet_id):
        """
        Clear the contents of an existing Google Sheet.
        """
        GSheetsHook(gcp_conn_id=gcp_conn_id).clear(
            spreadsheet_id=spreadsheet_id,
            range_="Sheet1",
        )

    @task
    def append_rows_to_google_sheet(spreadsheet_id, values):
        """
        Append rows to an existing Google Sheet.
        """
        GSheetsHook(gcp_conn_id=gcp_conn_id).append_values(
            spreadsheet_id=spreadsheet_id,
            range_="Sheet1",
            values=values,
            value_input_option="RAW",
        )

    # upload a file to Google Drive ("task_id" will be the name of the task in the Airflow UI)
    # if you need to setup dependencies, you can assign the return result to a variable, which is then
    # referenced in future steps, using the >> syntax
    LocalFilesystemToGoogleDriveOperator(
        gcp_conn_id=gcp_conn_id,
        task_id="upload_fie_to_drive",
        local_paths=["/opt/airflow/rialto_airflow/dags/harvest.py"],
        folder_id=google_drive_id,
        drive_folder="",
    )

    sheet_cleared = clear_google_sheet(google_sheet_id)

    sheet_cleared >> append_rows_to_google_sheet(
        google_sheet_id,
        [
            [
                "Hello",
                "World",
                datetime.date.today().isoformat(),
                datetime.datetime.now().isoformat(),
            ]
        ],
    )


google()
