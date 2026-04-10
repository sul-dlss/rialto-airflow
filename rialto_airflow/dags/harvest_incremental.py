import datetime
import logging
from pathlib import Path

from airflow.models import Param, Variable
from airflow.sdk import dag, task, task_group, get_current_context

from rialto_airflow import funders
from rialto_airflow.harvest_incremental import (
    altmetric,
    authors,
    crossref,
    deduplicate,
    dimensions,
    distill,
    openalex,
    pubmed,
    sul_pub,
    wos,
)
from rialto_airflow.honeybadger import default_args
from rialto_airflow.schema.rialto import RIALTO_DB_NAME, Harvest

data_dir = Path(Variable.get("data_dir"))
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
    logging.warning(
        f"⚠️ harvest_limit is set to {harvest_limit}, running harvest will stop at the limit number of publications per source"
    )
else:
    logging.debug(
        "‼️ no harvest_limit is set, running harvest will attempt to retrieve all results"
    )


@dag(
    schedule="@weekly",
    max_active_runs=1,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args(),
    params={
        "full_harvest": Param(
            False,
            type="boolean",
            title="Run a full harvest",
            description="Collect publications from data sources irrespective of when they were added/updated. This will effectively update all the platform metadata.",
        )
    },
)
def harvest_incremental():
    @task()
    def setup():
        """
        Create a Harvest record to track this run.
        """
        context = get_current_context()
        if context["params"]["full_harvest"]:
            harvest = Harvest.create(is_full=True)
            logging.info(f"Created full harvest id={harvest.id}")
        else:
            harvest = Harvest.create()
            logging.info(f"Created incremental harvest id={harvest.id}")

        return harvest.id

    @task()
    def load_authors(harvest_id):
        """
        Load the authors data from the authors CSV into the database.
        """
        authors.load_authors_table(data_dir)
        authors.clear_pub_author_links(harvest_id)

    @task()
    def dimensions_harvest(harvest_id):
        """
        Fetch the data by ORCID from Dimensions.
        """
        dimensions.harvest(harvest_id, limit=harvest_limit)

    @task()
    def openalex_harvest(harvest_id):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        openalex.harvest(harvest_id, limit=harvest_limit)

    @task()
    def wos_harvest(harvest_id):
        """
        Fetch the data by ORCID from Web of Science.
        """
        wos.harvest(harvest_id, limit=harvest_limit)

    @task()
    def sulpub_harvest(harvest_id):
        """
        Harvest data from SUL-Pub.
        """
        sul_pub.harvest(sul_pub_host, sul_pub_key, harvest_id, limit=harvest_limit)

    @task()
    def pubmed_harvest(harvest_id):
        """
        Fetch the data by ORCID from Pubmed.
        """
        pubmed.harvest(harvest_id, limit=harvest_limit)

    @task_group()
    def harvest_pubs(harvest_id):
        dimensions_harvest(harvest_id)
        openalex_harvest(harvest_id)
        wos_harvest(harvest_id)
        sulpub_harvest(harvest_id)
        pubmed_harvest(harvest_id)

    @task()
    def fill_in_openalex(harvest_id):
        """
        Fill in OpenAlex data for DOIs from other publication sources.
        """
        openalex.fill_in(harvest_id)

    @task()
    def fill_in_dimensions(harvest_id):
        """
        Fill in Dimensions data for DOIs from other publication sources.
        """
        dimensions.fill_in(harvest_id)

    @task()
    def fill_in_wos(harvest_id):
        """
        Fill in WebOfScience data for DOIs from other publication sources.
        """
        wos.fill_in(harvest_id)

    @task()
    def fill_in_pubmed(harvest_id):
        """
        Fill in Pubmed data for DOIs from other publication sources.
        """
        pubmed.fill_in(harvest_id)

    @task()
    def fill_in_crossref(harvest_id):
        """
        Fill in Crossref data for DOIs from other publication sources.
        """
        crossref.fill_in(harvest_id)

    @task()
    def fill_in_altmetric(harvest_id):
        """
        Fill in Altmetric data for all DOIs.
        """
        altmetric.fill_in(harvest_id)

    @task_group()
    def fill_in(harvest_id):
        fill_in_openalex(harvest_id)
        fill_in_dimensions(harvest_id)
        fill_in_wos(harvest_id)
        fill_in_crossref(harvest_id)
        fill_in_pubmed(harvest_id)
        fill_in_altmetric(harvest_id)

    @task()
    def remove_duplicates(harvest_id):
        """
        Remove duplicates. This task is run *before* the distill_publications
        task because we want to fold together any duplicates prior to extracting
        values from metadata.
        """
        deduplicate.remove_duplicates()

    @task()
    def distill_publications(harvest_id):
        """
        Distill the publication metadata into publication table columns.
        """
        distill.distill()

    @task()
    def link_funders(harvest_id):
        """
        Link all the publications to funders.
        """
        funders.link_publications(RIALTO_DB_NAME)

    @task()
    def remove_orphan_pubs(harvest_id):
        """
        Remove publications that have no linked authors.
        """
        deduplicate.remove_orphan_publications()

    @task_group()
    def post_process(harvest_id):
        dedupe = remove_duplicates(harvest_id)
        distill = distill_publications(harvest_id)
        link = link_funders(harvest_id)
        orphans = remove_orphan_pubs(harvest_id)
        dedupe >> [distill, link] >> orphans

    @task()
    def complete(harvest_id):
        harvest = Harvest.get_by_id(harvest_id)
        harvest.complete()

    # link up dag tasks and task groups

    harvest_id = setup()

    (
        load_authors(harvest_id)
        >> harvest_pubs(harvest_id)
        >> fill_in(harvest_id)
        >> post_process(harvest_id)
        >> complete(harvest_id)
    )


harvest_incremental()
