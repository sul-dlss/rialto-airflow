from io import StringIO

import pandas
from typer.testing import CliRunner

from rialto_airflow.cli import app

runner = CliRunner()


def test_publications(test_incremental_session, dataset_incremental):
    result = runner.invoke(app, ["publications", "janes"])
    assert result.exit_code == 0

    df = pandas.read_csv(StringIO(result.output))
    assert len(df) == 1
    assert len(df.columns) == 10

    row = df.iloc[0]
    assert row.doi == "10.000/000001"
    assert row.title == "My Life"
    assert row.pub_year == 2023
    assert row.sources == "sulpub|crossref|dim|wos|openalex|pubmed"


def test_publications_no_author(test_incremental_session, dataset_incremental):
    result = runner.invoke(app, ["publications", "fiddlesticks"])
    assert result.exit_code == 1
    assert result.output.strip() == "The author fiddlesticks does not exist"


def test_authors(test_incremental_session, dataset_incremental):
    result = runner.invoke(app, ["authors"])
    assert result.exit_code == 0
    assert "janes" in result.output
