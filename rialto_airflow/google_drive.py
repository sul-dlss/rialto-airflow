from airflow.providers.google.cloud.transfers.local_to_gcs import GoogleDriveUploadBlobOperator

def transfer_to_google_drive(filename, drive_folder_id):
    GoogleDriveUploadBlobOperator(
        task_id='upload_to_drive',
        local_file_path=filename,
        drive_file_id=drive_folder_id,
        drive_file_name=filename,
        gcp_conn_id="google_cloud",
    )
