import datetime
import logging
from pathlib import Path
import shutil
import os

from airflow.decorators import dag, task, task_group
from airflow.models import Variable

from rialto_airflow import funders
from rialto_airflow.harvest import (
    authors,
    crossref,
    dimensions,
    openalex,
    sul_pub,
    wos,
    pubmed,
    distill,
    deduplicate,
)
from rialto_airflow.database import create_database, create_schema, HarvestSchemaBase
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import rialto_authors_file
from rialto_airflow.honeybadger import default_args

gcp_conn_id = Variable.get("google_connection")
data_dir = Path(Variable.get("data_dir"))
sul_pub_host = Variable.get("sul_pub_host")
sul_pub_key = Variable.get("sul_pub_key")
google_drive_id = Variable.get(
    "google_drive_id", os.environ.get("AIRFLOW_TEST_GOOGLE_DRIVE_ID")
)

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


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
)
def harvest():
    @task()
    def setup():
        """
        Set up the snapshot directory and database.
        """
        snapshot = Snapshot.create(data_dir)
        shutil.copyfile(Path(rialto_authors_file(data_dir)), snapshot.authors_csv)
        create_database(snapshot.database_name)
        create_schema(snapshot.database_name, HarvestSchemaBase)

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

    @task()
    def fill_in_crossref(snapshot):
        """
        Fill in Crossref data for DOIs from other publication sources.
        """
        crossref.fill_in(snapshot)

    @task_group()
    def fill_in(snapshot):
        fill_in_openalex(snapshot)
        fill_in_dimensions(snapshot)
        fill_in_wos(snapshot)
        fill_in_crossref(snapshot)
        fill_in_pubmed(snapshot)

    @task()
    def remove_duplicates(snapshot):
        """
        Remove duplicates based on WOS UID.
        """
        deduplicate.remove_wos_duplicates(snapshot)

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
        remove_duplicates(snapshot)
        distill_publications(snapshot)
        link_funders(snapshot)

    @task()
    def complete(snapshot):
        snapshot.complete()

    # link up dag tasks and task groups

    snapshot = setup()

    (
        load_authors(snapshot)
        >> harvest_pubs(snapshot)
        >> fill_in(snapshot)
        >> post_process(snapshot)
        >> complete(snapshot)
    )


harvest()
