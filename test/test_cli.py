import json
import os
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


def test_export(test_incremental_session, dataset_incremental):
    result = runner.invoke(app, ["export", "sulpub"])
    assert result.exit_code == 0

    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 1, "one line per publication with sulpub json"

    row = json.loads(lines[0])
    assert row["sulpubid"] == "123456"
    assert row["title"] == "Sometimes limes are ok"


def test_export_to_file(test_incremental_session, dataset_incremental, tmp_path):
    output = tmp_path / "wos.jsonl"
    result = runner.invoke(app, ["export", "wos", "--output", str(output)])
    assert result.exit_code == 0

    lines = [line for line in output.read_text().splitlines() if line.strip()]
    assert len(lines) == 1, "one line per publication with wos json"
    assert json.loads(lines[0]), "each line is valid json"


def test_export_unknown_provider(test_incremental_session, dataset_incremental):
    result = runner.invoke(app, ["export", "bogus"])
    assert result.exit_code == 1


def test_database_url_option(test_incremental_session, dataset_incremental):
    # pass the test database connection string explicitly via --database-url and
    # confirm a command still runs against it
    database_url = os.environ["AIRFLOW_VAR_RIALTO_POSTGRES"]
    result = runner.invoke(app, ["--database-url", database_url, "authors"])
    assert result.exit_code == 0
    assert "janes" in result.output
