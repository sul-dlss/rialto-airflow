# A module containing methods to interact with Google Sheets and Google Drive using Airflow operators.
# Use these methods in your own DAGs.
# Configuration and authentication to google is described in the README.

from airflow.providers.google.suite.transfers.local_to_drive import (
    LocalFilesystemToGoogleDriveOperator,
)
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.models import Variable

gcp_conn_id = Variable.get("google_connection")


def clear_google_sheet(spreadsheet_id):
    """
    Clear the contents of an existing Google Sheet.
    Provide google sheet ID.
    The service account must have access to the sheet.
    """
    GSheetsHook(gcp_conn_id=gcp_conn_id).clear(
        spreadsheet_id=spreadsheet_id,
        range_="Sheet1",
    )


def append_rows_to_google_sheet(spreadsheet_id, values):
    """
    Append rows to an existing Google Sheet.
    Provide google sheet ID and an array of values to add to a new row.
    The service account must have access to the sheet.
    """
    GSheetsHook(gcp_conn_id=gcp_conn_id).append_values(
        spreadsheet_id=spreadsheet_id,
        range_="Sheet1",
        values=values,
        value_input_option="RAW",
    )


def replace_file_in_google_drive(local_filename, google_file_id):
    """
    Replace an existing file in Google Drive, and preserve the file ID.
    Provide the local filename (with path) and the file ID for the file in Google Drive to replace.
    The file ID must exist in google drive and the service account must have access to the file.
    """
    drive_hook = GoogleDriveHook(gcp_conn_id=gcp_conn_id)

    updated_file = (
        drive_hook.get_conn()
        .files()
        .update(
            fileId=google_file_id,
            media_body=local_filename,
            supportsAllDrives=True,
            fields="id",
        )
    )

    updated_file.execute()


def upload_file_to_google_drive(local_filename, google_drive_id):
    """
    Updload an new file to a Google Drive folder.
    Provide the local filename (with path) and the drive folder ID for the folder in Google Drive.
    The folder ID must exist in google drive and the service account must have access to the folder.
    If an existsing file of the same name exists in the folder, it will NOT be replaced, a new copy will be added.
    """
    operator = LocalFilesystemToGoogleDriveOperator(
        gcp_conn_id=gcp_conn_id,
        task_id="upload_file_to_drive",
        local_paths=[local_filename],
        folder_id=google_drive_id,
        drive_folder="",
    )

    return operator.execute(context={})
