import datetime

from airflow.decorators import dag, task
from airflow.providers.google.suite.transfers.local_to_drive import (
    LocalFilesystemToGoogleDriveOperator,
)
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

@dag(
    # schedule=@weekly,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def google():
    @task
    def publish_file_to_google_drive(local_filename, drive_folder_id):
        """
        Publish a file to a Google Drive folder.
        """
        LocalFilesystemToGoogleDriveOperator(
            gcp_conn_id="google_cloud_default",
            task_id="upload_to_drive",
            local_paths=[local_filename],
            folder_id=drive_folder_id,
            drive_folder="",
        ).execute(context={})

    @task
    def clear_google_sheet(spreadsheet_id):
        """
        Clear the contents of an existing Google Sheet.
        """
        hook = GSheetsHook(gcp_conn_id="google_cloud_default")
        hook.clear(
            spreadsheet_id=spreadsheet_id,
            range_="Sheet1",
        )

    @task
    def append_rows_to_google_sheet(spreadsheet_id, values):
        """
        Append rows to an existing Google Sheet.
        """
        hook = GSheetsHook(gcp_conn_id="google_cloud_default")
        hook.append_values(
            spreadsheet_id=spreadsheet_id,
            range_="Sheet1",
            values=values,
            value_input_option="RAW",
        )

    publish_file_to_google_drive("/opt/airflow/rialto_airflow/dags/google.py", "1xjxrCUrA0yrOt0i5wNTrWB-841GJ_VDL")

    clear_google_sheet(
        "1FKyGKzRu2M7Swd8pPCXG2Z2taiCtNkEFhwcIMJIPfi8")

    append_rows_to_google_sheet(
        "1FKyGKzRu2M7Swd8pPCXG2Z2taiCtNkEFhwcIMJIPfi8",
        [["Hello", "World", datetime.date.today().isoformat(), datetime.datetime.now().isoformat()]])


google()
