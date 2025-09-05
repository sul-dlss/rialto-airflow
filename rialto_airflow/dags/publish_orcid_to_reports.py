import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.database import (
    RIALTO_REPORTS_DB_NAME,
    create_database,
    database_exists,
)
from rialto_airflow.honeybadger import default_args
from rialto_airflow.publish import orcid

data_dir = Path(Variable.get("data_dir"))
"""
This DAG publishes data to postgres that is used to build ORCID dashboards
"""


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def publish_orcid_to_reports():
    @task()
    def setup_authors():
        # read authors data from the data_dir CSV.
        orcid.export_author_orcids()

    @task
    def publish():
        if not database_exists(RIALTO_REPORTS_DB_NAME):
            create_database(RIALTO_REPORTS_DB_NAME)

        # determine how to do this for orcids. Where should these methods live?
        # publication.init_reports_data_schema()
        # should this be in an orcid.py module?

        orcid.export_orcid_integration_stats()

    setup_authors() >> publish()


publish_orcid_to_reports()
