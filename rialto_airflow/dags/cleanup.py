import datetime
import os
import shutil
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from pathlib import Path

data_dir = Variable.get("data_dir")
cleanup_interval_days = Variable.get("cleanup_interval_days", default_var=30)
current_time = datetime.datetime.now()


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def cleanup():
    @task
    def cleanup_author_files():
        """
        Remove author files older than the specified number of days from the data directory.
        """
        logging.info(
            f"Cleanup any author files older than {cleanup_interval_days} days"
        )
        files = list(Path(data_dir).glob("authors.csv.*")) + list(
            Path(data_dir).glob("authors_active.csv.*")
        )
        for file in files:
            creation_time = os.path.getmtime(str(file.resolve()))
            file_time = datetime.datetime.fromtimestamp(creation_time)
            age = current_time - file_time
            if age.days > int(cleanup_interval_days):
                logging.info(f"Removing author file: {file} (age: {age.days} days)")
                file.unlink()
        return True

    @task
    def cleanup_snapshots():
        """
        Remove snapshot folders older than the specified number of days from the data directory.
        """
        logging.info(
            f"Cleanup any snapshot folders older than {cleanup_interval_days} days"
        )
        base_snapshot_folder = Path(data_dir) / "snapshots"
        snapshots = os.listdir(base_snapshot_folder)
        for snapshot in snapshots:
            folder_path = os.path.join(base_snapshot_folder, snapshot)
            if os.path.isdir(folder_path):
                creation_time = os.path.getmtime(folder_path)
                folder_time = datetime.datetime.fromtimestamp(creation_time)
                age = current_time - folder_time
                if age.days > int(cleanup_interval_days):
                    logging.info(
                        f"Removing snapshot folder: {folder_path} (age: {age.days} days)"
                    )
                    shutil.rmtree(folder_path)
        return True

    @task
    def cleanup_databases():
        """
        Remove snapshot databases older than the specified number of days.
        """
        logging.info(f"Cleanup any databases older than {cleanup_interval_days} days")
        return True

    cleanup_author_files()

    cleanup_snapshots()

    cleanup_databases()


cleanup()
