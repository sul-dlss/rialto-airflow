import logging
import shutil
from pathlib import Path
from datetime import datetime

from rialto_airflow.database import drop_database, database_names
import re


def cleanup_author_files(cleanup_interval_days: int, data_dir: str):
    current_time = datetime.now()

    files = list(Path(data_dir).glob("authors.csv.*")) + list(
        Path(data_dir).glob("authors_active.csv.*")
    )
    for file in files:
        date_str = file.name.split(".")[-1]
        try:
            file_time = datetime.strptime(date_str, "%m-%d-%Y")
            age = current_time - file_time
            if age.days > cleanup_interval_days:
                logging.info(f"Removing author file: {file} (age: {age.days} days)")
                file.unlink()
        except ValueError:
            logging.warning(
                f"Skipping file {file.name} as it does not match the expected date format"
            )


def cleanup_snapshots(cleanup_interval_days: int, data_dir: str):
    current_time = datetime.now()

    logging.debug(f"Current time: {current_time}")
    # first the data folders
    base_snapshot_folder = Path(data_dir) / "snapshots"
    for folder_path in base_snapshot_folder.iterdir():
        if folder_path.is_dir():
            logging.debug(f"Considering snapshot folder {folder_path}")
            folder_time = datetime.strptime(folder_path.name, "%Y%m%d%H%M%S")
            age = current_time - folder_time
            if age.days > cleanup_interval_days:
                logging.info(
                    f"Removing snapshot folder: {folder_path} (age: {age.days} days)"
                )
                try:
                    shutil.rmtree(folder_path)  # delete folder and all contents
                except Exception as exc:
                    logging.exception(
                        f"Failed to delete folder {folder_path} (error: {exc})"
                    )
    # next consider all of the databases (note: the `database_names` method already excludes postgres and airflow)
    for database_name in database_names():
        logging.debug(f"Considering database {database_name}")
        # Check if database name matches the expected format "rialto_%Y%m%d%H%M%S"
        if re.match(r"^rialto_\d{14}$", database_name):
            database_name_date_part = database_name.removeprefix("rialto_")
            database_time = datetime.strptime(database_name_date_part, "%Y%m%d%H%M%S")
            age = current_time - database_time
            if age.days > cleanup_interval_days:
                try:
                    logging.info(
                        f"Dropping database: {database_name} (age: {age.days} days)"
                    )
                    drop_database(database_name)
                except Exception as exc:
                    logging.exception(
                        f"Failed to drop database {database_name} (error: {exc})"
                    )
