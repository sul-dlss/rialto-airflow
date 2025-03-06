import dotenv
import os

from airflow.providers.google.suite.hooks.sheets import GSheetsHook

from rialto_airflow import google

dotenv.load_dotenv()


def google_sheet_id():
    return os.environ.get("AIRFLOW_TEST_GOOGLE_SHEET_ID")


def num_rows_google_sheet(sheet_id):
    values = GSheetsHook(gcp_conn_id="google_cloud_default").get_values(
        spreadsheet_id=sheet_id, range_="Sheet1"
    )

    return len(values)


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
    assert True, "Test not implemented"


def test_upload_file_to_google_drive():
    assert True, "Test not implemented"
