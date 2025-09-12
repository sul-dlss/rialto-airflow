import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.honeybadger import default_args
from rialto_airflow.publish import orcid


mais_base_url = Variable.get("mais_base_url")
mais_token_url = Variable.get("mais_token_url")
mais_client_id = Variable.get("mais_client_id")
mais_client_secret = Variable.get("mais_secret")
data_dir = Variable.get("data_dir")

"""
Publishes author data and integration stats to postgres for supporting the ORCID adoption dashboard
"""


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def publish_orcid_to_reports():
    @task
    def author_orcids():
        """
        Exports a subset of authors data to a table for ORCID adoption by role/department charts
        """
        orcid.export_author_orcids(data_dir)

        return True

    @task
    def orcid_integration_stats():
        """
        Exports latest ORCID integration counts to a table for reporting
        """
        orcid.export_orcid_integration_stats(
            mais_client_id, mais_client_secret, mais_token_url, mais_base_url
        )

        return True

    author_orcids() >> orcid_integration_stats()


publish_orcid_to_reports()
