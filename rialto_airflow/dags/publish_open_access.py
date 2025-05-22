import datetime
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable

import rialto_airflow.google as google
from rialto_airflow.honeybadger import default_args
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.publish import openaccess

data_dir = Path(Variable.get("data_dir"))
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
def publish_open_access():
    @task()
    def get_snapshot():
        snapshot = Snapshot.get_latest(data_dir)
        if snapshot is None:
            raise Exception(f"Unable to find completed snapshot in {data_dir}")
        else:
            return snapshot

    @task()
    def publish(snapshot):
        openaccess.write_publications(snapshot)
        openaccess.write_contributions(snapshot)
        openaccess.write_contributions_by_school(snapshot)
        openaccess.write_contributions_by_department(snapshot)

    @task()
    def upload(snapshot):
        csv_files = [
            "publications.csv",
            "contributions.csv",
            "contributions-by-school.csv",
            "contributions-by-department.csv",
        ]

        google_folder_id = google.get_file_id(
            google_drive_id, openaccess.google_drive_folder()
        )

        for csv_file in csv_files:
            file_path = snapshot.path / openaccess.google_drive_folder() / csv_file

            google.upload_or_replace_file_in_google_drive(
                str(file_path), google_folder_id
            )

    snapshot = get_snapshot()

    publish(snapshot) >> upload(snapshot)


publish_open_access()
