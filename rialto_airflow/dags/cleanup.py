import datetime
import logging

from airflow.models import Variable
from airflow.decorators import dag, task
from rialto_airflow.cleanup import (
    cleanup_author_files,
    cleanup_snapshots,
)

data_dir = Variable.get("data_dir")
cleanup_interval_days = int(Variable.get("cleanup_interval_days"))


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def cleanup():
    @task
    def author_files():
        """
        Remove author files older than the specified number of days from the data directory.
        """
        logging.info(
            f"Cleanup any author files older than {cleanup_interval_days} days from {data_dir}"
        )
        cleanup_author_files(cleanup_interval_days, data_dir)
        return True

    @task
    def snapshots():
        """
        Remove snapshot folders and databases older than the specified number of days from the data directory.
        """
        logging.info(
            f"Cleanup any snapshot folders and databases older than {cleanup_interval_days} days from {data_dir}"
        )
        cleanup_snapshots(cleanup_interval_days, data_dir)
        return True

    author_files()

    snapshots()


cleanup()
