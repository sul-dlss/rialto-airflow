import datetime
import pickle
from pathlib import Path
import shutil

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.harvest import dimensions, merge_pubs, openalex
from rialto_airflow.harvest.doi_sunet import create_doi_sunet_pickle
from rialto_airflow.harvest.sul_pub import sul_pub_csv
from rialto_airflow.harvest.contribs import create_contribs
from rialto_airflow.utils import (
    create_database,
    create_snapshot_dir,
    rialto_authors_file,
)

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
        Setup the data directory.
        """
        snapshot_dir = create_snapshot_dir(data_dir)
        create_database(snapshot_dir)
        return snapshot_dir

    @task()
    def find_authors_csv():
        """
        Find and return the path to the rialto-orgs authors.csv snapshot.
        """
        return rialto_authors_file(data_dir)

    @task()
    def dimensions_harvest_dois(authors_csv, snapshot_dir):
        """
        Fetch the data by ORCID from Dimensions.
        """
        pickle_file = Path(snapshot_dir) / "dimensions-doi-orcid.pickle"
        dimensions.doi_orcids_pickle(authors_csv, pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def openalex_harvest_dois(authors_csv, snapshot_dir):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        pickle_file = Path(snapshot_dir) / "openalex-doi-orcid.pickle"
        openalex.doi_orcids_pickle(authors_csv, pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def sul_pub_harvest(snapshot_dir):
        """
        Harvest data from SUL-Pub.
        """
        csv_file = Path(snapshot_dir) / "sulpub.csv"
        sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, limit=dev_limit)

        return str(csv_file)

    @task()
    def create_doi_sunet(dimensions, openalex, sul_pub, authors, snapshot_dir):
        """
        Extract a mapping of DOI -> [SUNET] from the dimensions doi-orcid dict,
        openalex doi-orcid dict, SUL-Pub publications, and authors data.
        """
        pickle_file = Path(snapshot_dir) / "doi-sunet.pickle"
        create_doi_sunet_pickle(dimensions, openalex, sul_pub, authors, pickle_file)

        return str(pickle_file)

    @task()
    def dimensions_harvest_pubs(doi_sunet, snapshot_dir):
        """
        Harvest publication metadata from Dimensions using the dois from doi_sunet.
        """
        dois = list(pickle.load(open(doi_sunet, "rb")).keys())
        csv_file = Path(snapshot_dir) / "dimensions-pubs.csv"
        dimensions.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def openalex_harvest_pubs(doi_sunet, snapshot_dir):
        """
        Harvest publication metadata from OpenAlex using the dois from doi_set.
        """
        dois = list(pickle.load(open(doi_sunet, "rb")).keys())
        csv_file = Path(snapshot_dir) / "openalex-pubs.csv"
        openalex.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def merge_publications(sul_pub, openalex_pubs, dimensions_pubs, snapshot_dir):
        """
        Merge the OpenAlex, Dimensions and sul_pub data.
        """
        output = Path(snapshot_dir) / "publications.parquet"
        merge_pubs.merge(sul_pub, openalex_pubs, dimensions_pubs, output)
        return str(output)

    @task()
    def pubs_to_contribs(pubs, doi_sunet_pickle, authors_csv, snapshot_dir):
        """
        Get contributions from publications.
        """
        output = Path(snapshot_dir) / "contributions.parquet"
        create_contribs(pubs, doi_sunet_pickle, authors_csv, output)

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

    snapshot_dir = setup()

    authors_csv = find_authors_csv()

    sul_pub = sul_pub_harvest(snapshot_dir)

    dimensions_dois = dimensions_harvest_dois(authors_csv, snapshot_dir)

    openalex_dois = openalex_harvest_dois(authors_csv, snapshot_dir)

    doi_sunet = create_doi_sunet(
        dimensions_dois, openalex_dois, sul_pub, authors_csv, snapshot_dir
    )

    dimensions_pubs = dimensions_harvest_pubs(doi_sunet, snapshot_dir)

    openalex_pubs = openalex_harvest_pubs(doi_sunet, snapshot_dir)

    pubs = merge_publications(sul_pub, openalex_pubs, dimensions_pubs, snapshot_dir)

    contribs = pubs_to_contribs(pubs, doi_sunet, authors_csv, snapshot_dir)

    publish(contribs, pubs)


harvest()
