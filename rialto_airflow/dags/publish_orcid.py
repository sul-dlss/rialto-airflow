import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.mais import (
    get_token,
    current_orcid_users,
    get_orcid_stats,
)

data_dir = Variable.get("data_dir")
mais_base_url = Variable.get("mais_base_url")
mais_client_id = Variable.get("mais_client_id")
mais_client_secret = Variable.get("mais_secret")


@dag(
    # schedule=@weekly,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def publish_orcid():
    @task
    def orcid_integration_stats():
        """
        Get current ORCID integration stats from the ORCID integration API and write to file in Google Drive.
        """
        access_token = get_token(mais_client_id, mais_client_secret, mais_base_url)
        current_users = current_orcid_users(access_token)
        orcid_stats = get_orcid_stats(current_users)
        return orcid_stats

    orcid_integration_stats()


publish_orcid()
