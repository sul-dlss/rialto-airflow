import dotenv
import os
import pandas as pd
import io

from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

from rialto_airflow import google
import time

dotenv.load_dotenv()


###############################################
# Environment variables
def google_sheet_id():
    return os.environ.get("AIRFLOW_TEST_GOOGLE_SHEET_ID")


def gcp_conn_id():
    return os.environ.get("AIRFLOW_VAR_GOOGLE_CONNECTION")


def google_drive_id():
    return os.environ.get("AIRFLOW_TEST_GOOGLE_DRIVE_ID")


def google_drive_hook():
    return GoogleDriveHook(gcp_conn_id=gcp_conn_id()).get_conn()


###############################################
# Google API functions needed for tests
def num_rows_google_sheet(sheet_id):
    values = GSheetsHook(gcp_conn_id=gcp_conn_id()).get_values(
        spreadsheet_id=sheet_id, range_="Sheet1"
    )

    return len(values)


def google_file_exists(folder_id, filename):
    results = (
        google_drive_hook()
        .files()
        .list(
            q=f"'{folder_id}' in parents and name = '{filename}' and trashed = false",
            fields="files(id, name, mimeType)",
            spaces="drive",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )

    return len(results.get("files", [])) > 0


def delete_google_file(folder_id, filename):
    file_id = google.get_file_id(folder_id, filename)
    if file_id:
        google_drive_hook().files().update(
            fileId=file_id, supportsAllDrives=True, body={"trashed": True}
        ).execute()


def get_csv_file_contents(file_id):
    file = google_drive_hook().files().get_media(fileId=file_id).execute()
    file_content = io.BytesIO(file)
    return pd.read_csv(file_content)


###############################################
# Tests
def test_open_access_dashboard_folder_id():
    folder_id = google.open_access_dashboard_folder_id()
    assert folder_id is not None


def orcid_dashboard_folder_id():
    folder_id = google.orcid_dashboard_folder_id()
    assert folder_id is not None


def test_get_file_id():
    # Confirm the file exists in the shared google drive
    assert google_file_exists(google_drive_id(), "Test") is True
    # Confirm the file ID of the existing file
    file_id = google.get_file_id(google_drive_id(), "Test")
    assert file_id == google_sheet_id()

    # Confirm the file does not exist in the shared google drive
    assert google_file_exists(google_drive_id(), "bogus.csv") is False
    # Confirm the file ID of the non-existing file
    file_id = google.get_file_id(google_drive_id(), "bogus.csv")
    assert file_id is None


def test_append_rows_to_google_sheet():
    # Start with a clear sheet
    google.clear_google_sheet(google_sheet_id())

    # Add a row of data and verify we have one row
    google.append_rows_to_google_sheet(google_sheet_id(), [["Hello", "World"]])
    assert num_rows_google_sheet(google_sheet_id()) == 1

    # Add a second row of data and verify we have two rows
    google.append_rows_to_google_sheet(google_sheet_id(), [["Row", "Two"]])
    assert num_rows_google_sheet(google_sheet_id()) == 2


def test_clear_google_sheet():
    # Add a row of data and verify we have at least one row
    google.append_rows_to_google_sheet(google_sheet_id(), [["Hello", "World"]])
    assert num_rows_google_sheet(google_sheet_id()) > 0

    # Clear the sheet, and verify we have zero rows
    google.clear_google_sheet(google_sheet_id())
    assert num_rows_google_sheet(google_sheet_id()) == 0


def test_replace_file_in_google_drive():
    # Delete the file if it exists
    delete_google_file(google_drive_id(), "authors.csv")

    # Upload the file
    google.upload_file_to_google_drive("test/data/authors.csv", google_drive_id())

    # give it some time to upload to google before checking it exists
    time.sleep(4)

    # Get the file ID of the uploaded file and ensure it exists
    file_id = google.get_file_id(google_drive_id(), "authors.csv")
    assert file_id is not None

    # Confirm the file contents are as expected
    df = get_csv_file_contents(file_id)
    assert len(df) == 10  # 10 rows in the original file
    assert df.to_dict("records")[9]["sunetid"] == "sunet10"

    # Replace this file with a new one, preserving the file ID
    google.replace_file_in_google_drive("test/data/authors_updated.csv", file_id)

    # Confirm the old file still exists in the shared google drive
    assert google_file_exists(google_drive_id(), "authors.csv") is True

    # Confirm the updated filename does not exist in the shared google drive
    # (it should have replaced the existing file, keeping the same name and same ID)
    assert google_file_exists(google_drive_id(), "authors_updated.csv") is False

    # Confirm the file id contents are as expected, including the new row
    df = get_csv_file_contents(file_id)
    assert len(df) == 11  # 11 rows in the updated file
    assert df.to_dict("records")[10]["sunetid"] == "sunet11"

    # Clenup the test file
    delete_google_file(google_drive_id(), "authors.csv")


def test_upload_file_to_google_drive():
    # Delete the file if it exists
    delete_google_file(google_drive_id(), "authors.csv")

    # Confirm the file does not exist in the shared google drive
    assert google_file_exists(google_drive_id(), "authors.csv") is False

    # Upload the file
    google.upload_file_to_google_drive("test/data/authors.csv", google_drive_id())

    # give it some time to upload to google before checking it exists
    time.sleep(4)

    # Confirm the file now exists in the shared google drive
    assert google_file_exists(google_drive_id(), "authors.csv") is True

    # Clenup the test file
    delete_google_file(google_drive_id(), "authors.csv")
