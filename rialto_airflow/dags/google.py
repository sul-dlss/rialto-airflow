import datetime

from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.suite.transfers.local_to_drive import (
    LocalFilesystemToGoogleDriveOperator,
)
from airflow.providers.google.suite.operators.sheets import (
    GoogleSheetsCreateSpreadsheetOperator,
)
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.operators.bash import BashOperator
from airflow.providers.google.suite.transfers.gcs_to_sheets import (
    GCSToGoogleSheetsOperator,
)


@dag(
    # schedule=@weekly,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def google():
    @task
    def publish_file_to_google_drive():
        """
        Publish a file to Google Drive.
        """
        # transfer_to_google_drive(
        #     filename="/opt/airflow/rialto_airflow/google_drive.py",
        #     drive_folder_id="1xjxrCUrA0yrOt0i5wNTrWB-841GJ_VDL",
        # )
        filename = "/opt/airflow/rialto_airflow/dags/google.py"
        drive_folder_id = "1xjxrCUrA0yrOt0i5wNTrWB-841GJ_VDL"
        LocalFilesystemToGoogleDriveOperator(
            gcp_conn_id="google_cloud_default",
            task_id="upload_to_drive",
            local_paths=[filename],
            folder_id=drive_folder_id,
            drive_folder="",
        ).execute(context={})

    @task
    def create_google_sheet():
        """
        Create a Google Sheet.
        """
        spreadsheet = {
            "properties": {
                "title": "ORCID Integration Stats",
            },
            "sheets": [
                {
                    "properties": {
                        "title": "Sheet1",
                    },
                },
            ],
        }
        create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
            task_id="create_spreadsheet",
            spreadsheet=spreadsheet,
            gcp_conn_id="google_cloud_default",
        ).execute(context={})
        # print_spreadsheet_url = BashOperator(
        #     task_id="print_spreadsheet_url",
        #     bash_command=f"echo {XComArg(create_spreadsheet, key='spreadsheet_url')}",
        # )
        # print("Spreadsheet URL: ", print_spreadsheet_url)

    @task
    def append_rows_to_google_sheet(spreadsheet_id, values):
        """
        Append rows to an existing Google Sheet.
        """
        hook = GSheetsHook(gcp_conn_id="google_cloud_default")
        values = (values,)
        hook.append_values(
            spreadsheet_id=spreadsheet_id,
            range_="Sheet1!A1:D1",
            values=values,
            value_input_option="RAW",
        )

    @task
    def copy_csv_to_sheet(folder_id, spreadsheet_id, csv_file):
        """
        Copy CSV file to Google Sheet.
        """
        GCSToGoogleSheetsOperator(
            gcp_conn_id="google_cloud_default",
            task_id="copy_csv_to_sheet",
            bucket_name=folder_id,
            object_name=csv_file,
            spreadsheet_id=spreadsheet_id,
        ).execute(context={})

    publish_file_to_google_drive()

    create_google_sheet()

    append_rows_to_google_sheet(
        "1FKyGKzRu2M7Swd8pPCXG2Z2taiCtNkEFhwcIMJIPfi8", [["Hello", "World"]]
    )

    copy_csv_to_sheet(
        "1xjxrCUrA0yrOt0i5wNTrWB-841GJ_VDL",
        "1FKyGKzRu2M7Swd8pPCXG2Z2taiCtNkEFhwcIMJIPfi8",
        "orcid-integration-stats.csv",
    )


google()
