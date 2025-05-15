import datetime
import logging
from pathlib import Path
import shutil

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from honeybadger import honeybadger  # type: ignore

from rialto_airflow import funders
from rialto_airflow.harvest import (
    authors,
    dimensions,
    openalex,
    sul_pub,
    wos,
    pubmed,
    distill,
)
from rialto_airflow.publish import openaccess, data_quality
from rialto_airflow.database import create_database, create_schema
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import rialto_authors_file
import rialto_airflow.google as google

gcp_conn_id = Variable.get("google_connection")
data_dir = Variable.get("data_dir")
publish_dir = Variable.get("publish_dir")
sul_pub_host = Variable.get("sul_pub_host")
sul_pub_key = Variable.get("sul_pub_key")

# to artificially limit the API activity in development
harvest_limit = None
try:
    harvest_limit = int(Variable.get("harvest_limit", default_var=None))
except TypeError:
    pass
except ValueError:
    pass

if harvest_limit:
    logging.info(
        f"⚠️ harvest_limit is set to {harvest_limit}, running harvest will stop at the limit number of publications per source"
    )
else:
    logging.info(
        "‼️ no harvest_limit is set, running harvest will attempt to retrieve all results"
    )


honeybadger.configure(
    api_key=Variable.get("honeybadger_api_key"),
    environment=Variable.get("honeybadger_env"),
    force_sync=True,
)  # type: ignore


def task_failure_notify(context):
    task = context["task"].task_id
    logging.error(f"Task {task} failed.")
    honeybadger.notify(
        error_class="Task failure",
        error_message=f"Task {task} failed in {context.get('task_instance_key_str')}",
        context=context,
    )


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args={"on_failure_callback": task_failure_notify},
)
def harvest():
    @task()
    def setup():
        """
        Set up the snapshot directory and database.
        """
        snapshot = Snapshot(data_dir)
        shutil.copyfile(Path(rialto_authors_file(data_dir)), snapshot.authors_csv)
        create_database(snapshot.database_name)
        create_schema(snapshot.database_name)

        return snapshot

    @task()
    def load_authors(snapshot):
        """
        Load the authors data from the authors CSV into the database.
        """
        authors.load_authors_table(snapshot)

    @task()
    def dimensions_harvest(snapshot):
        """
        Fetch the data by ORCID from Dimensions.
        """
        dimensions.harvest(snapshot, limit=harvest_limit)

    @task()
    def openalex_harvest(snapshot):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        openalex.harvest(snapshot, limit=harvest_limit)

    @task()
    def wos_harvest(snapshot):
        """
        Fetch the data by ORCID from Web of Science.
        """
        wos.harvest(snapshot, limit=harvest_limit)

    @task()
    def sulpub_harvest(snapshot):
        """
        Harvest data from SUL-Pub.
        """
        sul_pub.harvest(snapshot, sul_pub_host, sul_pub_key, limit=harvest_limit)

    @task()
    def pubmed_harvest(snapshot):
        """
        Fetch the data by ORCID from Pubmed.
        """
        jsonl_file = pubmed.harvest(snapshot, limit=harvest_limit)

        return jsonl_file

    @task_group()
    def harvest_pubs(snapshot):
        dimensions_harvest(snapshot)
        openalex_harvest(snapshot)
        wos_harvest(snapshot)
        sulpub_harvest(snapshot)
        pubmed_harvest(snapshot)

    @task()
    def fill_in_openalex(snapshot):
        """
        Fill in OpenAlex data for DOIs from other publication sources.
        """
        openalex.fill_in(snapshot)

    @task()
    def fill_in_dimensions(snapshot):
        """
        Fill in Dimensions data for DOIs from other publication sources.
        """
        dimensions.fill_in(snapshot)

    @task()
    def fill_in_wos(snapshot):
        """
        Fill in WebOfScience data for DOIs from other publication sources.
        """
        wos.fill_in(snapshot)

    @task()
    def fill_in_pubmed(snapshot):
        """
        Fill in Pubmed data for DOIs from other publication sources.
        """
        pubmed.fill_in(snapshot)

    @task_group()
    def fill_in(snapshot):
        fill_in_openalex(snapshot)
        fill_in_dimensions(snapshot)
        fill_in_wos(snapshot)
        fill_in_pubmed(snapshot)

    @task()
    def distill_publications(snapshot):
        """
        Distill the publication metadata into publication table columns.
        """
        distill.distill(snapshot)

    @task()
    def link_funders(snapshot):
        """
        Link all the publications to funders.
        """
        funders.link_publications(snapshot)

    @task_group()
    def post_process(snapshot):
        distill_publications(snapshot)
        link_funders(snapshot)

    @task()
    def publish_open_access(snapshot):
        openaccess.write_publications(snapshot)
        openaccess.write_contributions(snapshot)
        openaccess.write_contributions_by_school(snapshot)
        openaccess.write_contributions_by_department(snapshot)

    @task()
    def publish_data_quality(snapshot):
        data_quality.write_authors(snapshot)
        data_quality.write_sulpub(snapshot)
        data_quality.write_contributions_by_source(snapshot)
        data_quality.write_publications(snapshot)

    @task_group()
    def publish(snapshot):
        publish_open_access(snapshot)
        publish_data_quality(snapshot)

    @task()
    def upload_open_access_files(snapshot):
        csv_files = [
            "publications.csv",
            "contributions.csv",
            "contributions-by-school.csv",
            "contributions-by-department.csv",
        ]

        google_folder_id = google.open_access_dashboard_folder_id()

        for csv_file in csv_files:
            file_path = snapshot.path / "open-access-dashboard" / csv_file

            google.upload_or_replace_file_in_google_drive(
                str(file_path), google_folder_id
            )

    @task()
    def upload_data_quality_files(snapshot):
        csv_files = [
            "authors.csv",
            "sulpub.csv",
            "contributions-by-source.csv",
            "publications.csv",
        ]

        google_folder_id = google.data_quality_dashboard_folder_id()

        for csv_file in csv_files:
            file_path = snapshot.path / "data-quality-dashboard" / csv_file

            google.upload_or_replace_file_in_google_drive(
                str(file_path), google_folder_id
            )

    @task_group()
    def upload(snapshot):
        upload_open_access_files(snapshot)
        upload_data_quality_files(snapshot)

    # link up dag tasks and task groups

    snapshot = setup()

    (
        load_authors(snapshot)
        >> harvest_pubs(snapshot)
        >> fill_in(snapshot)
        >> post_process(snapshot)
        >> publish(snapshot)
        >> upload(snapshot)
    )


harvest()
