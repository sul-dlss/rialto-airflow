import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.honeybadger import default_args
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.publish import publication

data_dir = Path(Variable.get("data_dir"))
"""
This DAG publishes data to postgres that is used to build dashboards
"""


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def publish_to_reports():
    @task()
    def get_snapshot():
        snapshot = Snapshot.get_latest(data_dir)
        if snapshot is None:
            raise Exception(f"Unable to find completed snapshot in {data_dir}")
        else:
            return snapshot

    @task
    def publish_publications(snapshot):
        publication.export_publications(snapshot)

    @task
    def publish_publications_by_school(snapshot):
        publication.export_publications_by_school(snapshot)

    @task
    def publish_publications_by_department(snapshot):
        publication.export_publications_by_department(snapshot)

    snapshot = get_snapshot()

    (
        publish_publications(snapshot)
        >> publish_publications_by_school(snapshot)
        >> publish_publications_by_department(snapshot)
    )


publish_to_reports()
