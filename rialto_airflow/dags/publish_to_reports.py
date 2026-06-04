import datetime
from pathlib import Path

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.timetables.trigger import CronTriggerTimetable

from rialto_airflow.honeybadger import default_args
from rialto_airflow.publish import publication

data_dir = Path(Variable.get("data_dir"))
"""
This DAG publishes data to postgres that is used to build dashboards
"""


@dag(
    schedule=CronTriggerTimetable(
        "0 1 * * 3", timezone="UTC"
    ),  # At 01:00 on Wednesdays
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def publish_to_reports():
    @task.short_circuit
    def check_harvest_complete():
        return publication.check_harvest_complete()

    @task
    def publish_publications():
        publication.export_publications()

    @task
    def publish_publications_by_school():
        publication.export_publications_by_school()

    @task
    def publish_publications_by_department():
        publication.export_publications_by_department()

    @task
    def publish_publications_by_author():
        publication.export_publications_by_author()

    @task
    def generate_download_files(data_dir):
        publication.generate_download_files(data_dir)

    (
        check_harvest_complete()
        >> publish_publications()
        >> publish_publications_by_school()
        >> publish_publications_by_department()
        >> publish_publications_by_author()
        >> generate_download_files(data_dir)
    )


publish_to_reports()
