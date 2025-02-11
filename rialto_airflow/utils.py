import csv
import datetime
import logging
import os
from pathlib import Path
import re

from sqlalchemy import create_engine, text


def create_database(snapshot_dir):
    """Create a DAG-specific database for publications and author/orgs data"""
    timestamp = Path(snapshot_dir).name
    database_name = f"rialto_{timestamp}"

    # set up the connection using the default postgres database
    # see discussion here: https://stackoverflow.com/questions/6506578/how-to-create-a-new-database-using-sqlalchemy
    # and https://docs.sqlalchemy.org/en/20/core/connections.html#understanding-the-dbapi-level-autocommit-isolation-level
    postgres_conn = f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/postgres"
    engine = create_engine(postgres_conn)
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text(f"create database {database_name}"))
        connection.close()

    logging.info(f"created database {database_name}")
    return database_name


def create_snapshot_dir(data_dir):
    snapshots_dir = Path(data_dir) / "snapshots"

    if not snapshots_dir.is_dir():
        snapshots_dir.mkdir()

    now = datetime.datetime.now()
    snapshot_dir = snapshots_dir / now.strftime("%Y%m%d%H%M%S")
    snapshot_dir.mkdir()

    return str(snapshot_dir)


def rialto_authors_file(data_dir):
    """Get the path to the rialto-orgs authors.csv"""
    authors_file = Path(data_dir) / "authors.csv"

    if authors_file.is_file():
        return str(authors_file)
    else:
        raise Exception(f"authors file missing at {authors_file}")


def rialto_authors_orcids(rialto_authors_file):
    """Extract the orcidid column from the authors.csv file"""
    orcids = []
    with open(rialto_authors_file, "r") as file:
        reader = csv.reader(file)
        header = next(reader)
        orcidid = header.index("orcidid")
        for row in reader:
            if row[orcidid]:
                orcids.append(row[orcidid])
    return orcids


def invert_dict(dict):
    """
    Inverting the dictionary so that DOI is the common key for all tasks.
    This adds some complexity here but reduces complexity in downstream tasks.
    """
    original_values = []
    for v in dict.values():
        original_values.extend(v)
    original_values = list(set(original_values))

    inverted_dict = {}
    for i in original_values:
        inverted_dict[i] = [k for k, v in dict.items() if i in v]

    return inverted_dict


def normalize_doi(doi):
    doi = doi.strip().lower()
    doi = doi.replace("https://doi.org/", "").replace("https://dx.doi.org/", "")
    doi = re.sub("^doi: ", "", doi)

    return doi
