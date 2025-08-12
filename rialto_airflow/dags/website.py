import datetime
import shutil
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow import website
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.honeybadger import default_args

data_dir = Path(Variable.get("data_dir"))


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def build_website():
    @task()
    def get_snapshot():
        snapshot = Snapshot.get_latest(data_dir)
        if snapshot is None:
            raise Exception(f"Unable to find completed snapshot in {data_dir}")
        else:
            return snapshot

    @task
    def build(snapshot):
        """
        Build report website.
        """
        return website.build(snapshot)

    @task
    def publish(snapshot):
        """
        Publish the built website so that the development nginx web server can show it.
        """
        target = data_dir / "website"

        shutil.copytree(snapshot.path / "website", target, dirs_exist_ok=True)

        return True

    snapshot = get_snapshot()

    build(snapshot) >> publish(snapshot)


build_website()
