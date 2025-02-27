from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator

def transfer_to_google_drive(filename, drive_folder_id):
    print(f"Uploading {filename} to Google Drive folder {drive_folder_id}")
    remote_ids = LocalFilesystemToGoogleDriveOperator(
        gcp_conn_id="google_cloud_default",
        task_id='upload_to_drive',
        local_paths=[filename],
        folder_id=drive_folder_id,
        drive_folder="",
    )
    remote_ids.execute(context={})
