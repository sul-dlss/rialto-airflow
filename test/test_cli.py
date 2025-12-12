import os
from io import StringIO

import pandas
import pytest
from typer.testing import CliRunner

from rialto_airflow.cli import app

runner = CliRunner()


def test_publications(test_session, snapshot, dataset):
    result = runner.invoke(app, ["publications", "janes", "--db-name", "rialto_test"])
    assert result.exit_code == 0

    df = pandas.read_csv(StringIO(result.output))
    assert len(df) == 2
    assert len(df.columns) == 10

    row = df.iloc[0]
    assert row.doi == "10.000/000001"
    assert row.title == "My Life"
    assert row.pub_year == 2023
    assert row.sources == "sulpub|crossref|dim|wos|openalex|pubmed"

    row = df.iloc[1]
    assert row.doi == "10.000/000002"
    assert row.title == "My Life Part 2"
    assert row.pub_year == 2024
    assert row.sources == "sulpub|dim|wos|pubmed"


def test_publications_no_author(test_session, snapshot, dataset):
    result = runner.invoke(
        app, ["publications", "fiddlesticks", "--db-name", "rialto_test"]
    )
    assert result.exit_code == 1
    assert result.output.strip() == "The author fiddlesticks does not exist"


@pytest.fixture()
def empty_data_dir(tmp_path):
    os.environ["AIRFLOW_VAR_DATA_DIR"] = str(tmp_path)


def test_publications_no_db_name(empty_data_dir):
    result = runner.invoke(app, ["publications", "janes"])
    assert result.exit_code == 1


def test_authors_no_db_name(empty_data_dir):
    result = runner.invoke(app, ["authors"])
    assert result.exit_code == 1


def test_authors(test_session, snapshot, dataset):
    result = runner.invoke(app, ["authors", "--db-name", "rialto_test"])
    assert result.exit_code == 0
    assert "janes" in result.output
