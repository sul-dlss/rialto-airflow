import datetime
import pickle
from pathlib import Path
import shutil

from airflow.decorators import dag, task
from airflow.models import Variable

from rialto_airflow.harvest import authors, dimensions, merge_pubs, openalex
from rialto_airflow.harvest.doi_sunet import create_doi_sunet_pickle
from rialto_airflow.harvest import sul_pub
from rialto_airflow.harvest.contribs import create_contribs
from rialto_airflow.database import create_database, create_schema
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import rialto_authors_file


data_dir = Variable.get("data_dir")
publish_dir = Variable.get("publish_dir")
sul_pub_host = Variable.get("sul_pub_host")
sul_pub_key = Variable.get("sul_pub_key")

# to artificially limit the API activity in development
dev_limit = None
try:
    dev_limit = int(Variable.get("dev_limit", default_var=None))
except TypeError:
    pass
except ValueError:
    pass


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
    def dimensions_harvest_dois(snapshot):
        """
        Fetch the data by ORCID from Dimensions.
        """
        pickle_file = snapshot.path / "dimensions-doi-orcid.pickle"
        dimensions.doi_orcids_pickle(snapshot.authors_csv, pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def openalex_harvest_dois(snapshot):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        pickle_file = snapshot.path / "openalex-doi-orcid.pickle"
        openalex.doi_orcids_pickle(snapshot.authors_csv, pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def sul_pub_harvest(snapshot):
        """
        Harvest data from SUL-Pub.
        """
        jsonl_file = sul_pub.harvest(
            snapshot, sul_pub_host, sul_pub_key, limit=dev_limit
        )

        return jsonl_file

    @task()
    def create_doi_sunet(dimensions, openalex, sul_pub, snapshot):
        """
        Extract a mapping of DOI -> [SUNET] from the dimensions doi-orcid dict,
        openalex doi-orcid dict, SUL-Pub publications, and authors data.
        """
        pickle_file = snapshot.path / "doi-sunet.pickle"
        create_doi_sunet_pickle(
            dimensions, openalex, sul_pub, snapshot.authors_csv, pickle_file
        )

        return str(pickle_file)

    @task()
    def dimensions_harvest_pubs(doi_sunet, snapshot):
        """
        Harvest publication metadata from Dimensions using the dois from doi_sunet.
        """
        dois = list(pickle.load(open(doi_sunet, "rb")).keys())
        csv_file = snapshot.path / "dimensions-pubs.csv"
        dimensions.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def openalex_harvest_pubs(doi_sunet, snapshot):
        """
        Harvest publication metadata from OpenAlex using the dois from doi_set.
        """
        dois = list(pickle.load(open(doi_sunet, "rb")).keys())
        csv_file = snapshot.path / "openalex-pubs.csv"
        openalex.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def merge_publications(sul_pub, openalex_pubs, dimensions_pubs, snapshot):
        """
        Merge the OpenAlex, Dimensions and sul_pub data.
        """
        output = snapshot.path / "publications.parquet"
        merge_pubs.merge(sul_pub, openalex_pubs, dimensions_pubs, output)
        return str(output)

    @task()
    def pubs_to_contribs(pubs, doi_sunet_pickle, snapshot):
        """
        Get contributions from publications.
        """
        contribs_path = snapshot.path / "contributions.parquet"
        create_contribs(pubs, doi_sunet_pickle, snapshot.authors_csv, contribs_path)

        return str(contribs_path)

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

    snapshot = setup()

    snapshot = load_authors(snapshot)

    sul_pub_jsonl = sul_pub_harvest(snapshot)

    dimensions_dois = dimensions_harvest_dois(snapshot)

    openalex_dois = openalex_harvest_dois(snapshot)

    doi_sunet = create_doi_sunet(
        dimensions_dois,
        openalex_dois,
        sul_pub_jsonl,
        snapshot,
    )

    dimensions_pubs = dimensions_harvest_pubs(doi_sunet, snapshot)

    openalex_pubs = openalex_harvest_pubs(doi_sunet, snapshot)

    pubs = merge_publications(sul_pub, openalex_pubs, dimensions_pubs, snapshot)

    contribs = pubs_to_contribs(pubs, doi_sunet, snapshot)

    publish(contribs, pubs)


harvest()
