import datetime
import os
import shutil
import logging

from pathlib import Path
from rialto_airflow.database import drop_database, database_exists


current_time = datetime.datetime.now()


def cleanup_author_files(cleanup_interval_days, data_dir):
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


def cleanup_snapshots(cleanup_interval_days, data_dir):
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
                shutil.rmtree(folder_path)  # delete folder and all contents

                database_name = f"rialto_{snapshot}"
                logging.info(f"Searching for database named {database_name}")
                if database_exists(database_name):
                    drop_database(database_name)
