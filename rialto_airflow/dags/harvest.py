import datetime
import pickle
from pathlib import Path
import shutil

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.harvest import authors, dimensions, merge_pubs, openalex
from rialto_airflow.harvest.doi_sunet import create_doi_sunet_pickle
from rialto_airflow.harvest.sul_pub import sul_pub_csv
from rialto_airflow.harvest.contribs import create_contribs
from rialto_airflow.database import create_database, create_schema
from rialto_airflow.utils import create_snapshot_dir, rialto_authors_file


data_dir = Variable.get("data_dir")
publish_dir = Variable.get("publish_dir")
sul_pub_host = Variable.get("sul_pub_host")
sul_pub_key = Variable.get("sul_pub_key")

# to artificially limit the API activity in development
try:
    dev_limit = int(Variable.get("dev_limit", default_var=None))
except TypeError:
    dev_limit = None


@dag(
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def harvest():
    @task()
    def setup():
        """
        Set up the snapshot directory and database.
        """
        snapshot_dir = create_snapshot_dir(data_dir)
        shutil.copyfile(
            Path(rialto_authors_file(data_dir)), Path(snapshot_dir) / "authors.csv"
        )
        database_name = create_database(snapshot_dir)
        create_schema(database_name)

        harvest_config = {"snapshot_dir": snapshot_dir, "database_name": database_name}
        return harvest_config

    @task()
    def load_authors(harvest_config):
        """
        Load the authors data from the authors CSV into the database.
        """
        authors.load_authors_table(harvest_config)
        snapshot_dir = harvest_config["snapshot_dir"]
        return snapshot_dir

    @task()
    def dimensions_harvest_dois(harvest_config):
        """
        Fetch the data by ORCID from Dimensions.
        """
        pickle_file = (
            Path(harvest_config["snapshot_dir"]) / "dimensions-doi-orcid.pickle"
        )
        dimensions.doi_orcids_pickle(pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def openalex_harvest_dois(harvest_config):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        pickle_file = Path(harvest_config["snapshot_dir"]) / "openalex-doi-orcid.pickle"
        openalex.doi_orcids_pickle(pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def sul_pub_harvest(harvest_config):
        """
        Harvest data from SUL-Pub.
        """
        csv_file = Path(harvest_config["snapshot_dir"]) / "sulpub.csv"
        sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, limit=dev_limit)

        return str(csv_file)

    @task()
    def create_doi_sunet(dimensions, openalex, sul_pub, authors, harvest_config):
        """
        Extract a mapping of DOI -> [SUNET] from the dimensions doi-orcid dict,
        openalex doi-orcid dict, SUL-Pub publications, and authors data.
        """
        pickle_file = Path(harvest_config["snapshot_dir"]) / "doi-sunet.pickle"
        create_doi_sunet_pickle(dimensions, openalex, sul_pub, authors, pickle_file)

        return str(pickle_file)

    @task()
    def dimensions_harvest_pubs(doi_sunet, harvest_config):
        """
        Harvest publication metadata from Dimensions using the dois from doi_sunet.
        """
        dois = list(pickle.load(open(doi_sunet, "rb")).keys())
        csv_file = Path(harvest_config["snapshot_dir"]) / "dimensions-pubs.csv"
        dimensions.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def openalex_harvest_pubs(doi_sunet, harvest_config):
        """
        Harvest publication metadata from OpenAlex using the dois from doi_set.
        """
        dois = list(pickle.load(open(doi_sunet, "rb")).keys())
        csv_file = Path(harvest_config["snapshot_dir"]) / "openalex-pubs.csv"
        openalex.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def merge_publications(sul_pub, openalex_pubs, dimensions_pubs, harvest_config):
        """
        Merge the OpenAlex, Dimensions and sul_pub data.
        """
        output = Path(harvest_config["snapshot_dir"]) / "publications.parquet"
        merge_pubs.merge(sul_pub, openalex_pubs, dimensions_pubs, output)
        return str(output)

    @task()
    def pubs_to_contribs(pubs, doi_sunet_pickle, harvest_config):
        """
        Get contributions from publications.
        """
        output = Path(harvest_config["snapshot_dir"]) / "contributions.parquet"
        create_contribs(pubs, doi_sunet_pickle, output)

        return str(output)

    @task()
    def publish(pubs_to_contribs, merge_publications):
        """
        Publish aggregate data to JupyterHub environment.
        """
        contribs_path = Path(publish_dir) / "contributions.parquet"
        pubs_path = Path(publish_dir) / "publications.parquet"

        shutil.copyfile(pubs_to_contribs, contribs_path)
        shutil.copyfile(merge_publications, pubs_path)

        return str(publish_dir)

    harvest_config = setup()

    authors_table = load_authors(harvest_config)

    sul_pub = sul_pub_harvest(harvest_config)

    dimensions_dois = dimensions_harvest_dois(harvest_config)

    openalex_dois = openalex_harvest_dois(harvest_config)

    doi_sunet = create_doi_sunet(
        dimensions_dois,
        openalex_dois,
        sul_pub,
        harvest_config,
    )

    dimensions_pubs = dimensions_harvest_pubs(doi_sunet, harvest_config)

    openalex_pubs = openalex_harvest_pubs(doi_sunet, harvest_config)

    pubs = merge_publications(sul_pub, openalex_pubs, dimensions_pubs, harvest_config)

    contribs = pubs_to_contribs(pubs, doi_sunet, harvest_config)

    publish(contribs, pubs)


harvest()
