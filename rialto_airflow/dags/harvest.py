import datetime
import logging
from pathlib import Path
import shutil

from airflow.decorators import dag, task
from airflow.models import Variable
from honeybadger import honeybadger  # type: ignore

from rialto_airflow import funders
from rialto_airflow.harvest import authors, dimensions, openalex, sul_pub, wos, distill
from rialto_airflow.publish import openaccess
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
        return snapshot

    @task()
    def dimensions_harvest(snapshot):
        """
        Fetch the data by ORCID from Dimensions.
        """
        jsonl_file = dimensions.harvest(snapshot, limit=harvest_limit)

        return jsonl_file

    @task()
    def openalex_harvest(snapshot):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        jsonl_file = openalex.harvest(snapshot, limit=harvest_limit)

        return jsonl_file

    @task()
    def wos_harvest(snapshot):
        """
        Fetch the data by ORCID from Web of Science.
        """
        jsonl_file = wos.harvest(snapshot, limit=harvest_limit)

        return jsonl_file

    @task()
    def sul_pub_harvest(snapshot):
        """
        Harvest data from SUL-Pub.
        """
        jsonl_file = sul_pub.harvest(
            snapshot, sul_pub_host, sul_pub_key, limit=harvest_limit
        )

        return jsonl_file

    @task()
    def fill_in_openalex(
        snapshot, sul_pub_jsonl, openalex_jsonl, dimensions_jsonl, wos_jsonl
    ):
        """
        Fill in OpenAlex data for DOIs from other publication sources.
        """
        openalex.fill_in(snapshot, openalex_jsonl)

        return snapshot

    @task()
    def fill_in_dimensions(snapshot, openalex_jsonl, dimensions_jsonl, wos_jsonl):
        """
        Fill in Dimensions data for DOIs from other publication sources.
        """
        dimensions.fill_in(snapshot, dimensions_jsonl)

        return snapshot

    @task()
    def distill_publications(snapshot, openalex_fill_in, dimensions_fill_in):
        """
        Distill the publication metadata into publication table columns.
        """
        count = distill.distill(snapshot)

        return count

    @task()
    def link_funders(snapshot, openalex_fill_in, dimensions_fill_in):
        """
        Link all the publications to funders.
        """
        count = funders.link_publications(snapshot)

        return count

    @task()
    def publish_openaccess(snapshot):
        openaccess.write_publications(snapshot)
        openaccess.write_contributions(snapshot)

    @task()
    def upload_publish_files(snapshot):
        open_access_publications_filename = "publications.csv"
        open_access_contributions_filename = "contributions.csv"
        open_access_publications_file_id = google.get_file_id(
            google.open_access_dashboard_folder_id(), open_access_publications_filename
        )
        open_access_contributions_file_id = google.get_file_id(
            google.open_access_dashboard_folder_id(), open_access_contributions_filename
        )

        logging.info(
            f"Uploading {snapshot.path / open_access_publications_filename} to file id {open_access_publications_file_id}"
        )
        google.replace_file_in_google_drive(
            str(snapshot.path / open_access_publications_filename),
            open_access_publications_file_id,
        )

        logging.info(
            f"Uploading {snapshot.path / open_access_contributions_filename} to file id {open_access_contributions_file_id}"
        )
        google.replace_file_in_google_drive(
            str(snapshot.path / open_access_contributions_filename),
            open_access_contributions_file_id,
        )

    snapshot = setup()

    snapshot = load_authors(snapshot)

    sul_pub_jsonl = sul_pub_harvest(snapshot)

    dimensions_jsonl = dimensions_harvest(snapshot)

    openalex_jsonl = openalex_harvest(snapshot)

    wos_jsonl = wos_harvest(snapshot)

    openalex_fill_in = fill_in_openalex(
        snapshot, sul_pub_jsonl, openalex_jsonl, dimensions_jsonl, wos_jsonl
    )

    dimensions_fill_in = fill_in_dimensions(
        snapshot, openalex_jsonl, dimensions_jsonl, wos_jsonl
    )

    distilled_pubs = distill_publications(
        snapshot, openalex_fill_in, dimensions_fill_in
    )

    linked_pubs = link_funders(snapshot, openalex_fill_in, dimensions_fill_in)

    (
        (distilled_pubs, linked_pubs)
        >> publish_openaccess(snapshot)
        >> upload_publish_files(snapshot)
    )


harvest()
