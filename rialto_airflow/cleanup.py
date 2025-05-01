import logging
import shutil
from pathlib import Path
from datetime import datetime

from rialto_airflow.database import database_exists, drop_database


def cleanup_author_files(cleanup_interval_days: int, data_dir: str):
    current_time = datetime.now()

    files = list(Path(data_dir).glob("authors.csv.*")) + list(
        Path(data_dir).glob("authors_active.csv.*")
    )
    for file in files:
        file_time = datetime.strptime(file.name.split(".")[-1], "%Y%m%d")
        age = current_time - file_time
        if age.days > cleanup_interval_days:
            logging.info(f"Removing author file: {file} (age: {age.days} days)")
            file.unlink()


def cleanup_snapshots(cleanup_interval_days: int, data_dir: str):
    current_time = datetime.now()

    base_snapshot_folder = Path(data_dir) / "snapshots"
    for folder_path in base_snapshot_folder.iterdir():
        if folder_path.is_dir():
            folder_time = datetime.strptime(folder_path.name, "%Y%m%d%H%M%S")
            age = current_time - folder_time
            if age.days > cleanup_interval_days:
                logging.info(
                    f"Removing snapshot folder: {folder_path} (age: {age.days} days)"
                )
                shutil.rmtree(folder_path)  # delete folder and all contents

                database_name = f"rialto_{folder_path.name}"
                logging.info(f"Searching for database named {database_name}")
                if database_exists(database_name):
                    drop_database(database_name)
