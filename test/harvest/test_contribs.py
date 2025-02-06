import pickle
import pytest
import polars as pl

from rialto_airflow.harvest.contribs import create_contribs


@pytest.fixture
def pubs_parquet(tmp_path):
    fixture_path = tmp_path / "pubs.parquet"
    df = pl.DataFrame(
        {
            "doi": [
                "0000/abc",
                "0000/123",
                "0000/999",
            ],
            "title": ["Exquisite article", "Fantabulous research", "Perfect prose"],
        }
    )
    df.write_parquet(fixture_path)
    return str(fixture_path)


@pytest.fixture
def doi_sunet(tmp_path):
    fixture_path = tmp_path / "doi-sunet.pickle"
    m = {"0000/abc": ["user1"], "0000/123": ["user2"], "0000/999": ["user1", "user2"]}
    pickle.dump(m, open(str(fixture_path), "wb"))
    return str(fixture_path)


@pytest.fixture
def authors(tmp_path):
    fixture_path = tmp_path / "users.csv"
    df = pl.DataFrame(
        {"sunetid": ["user1", "user2"], "first_name": ["Mary", "Frederico"]}
    )
    df.write_csv(fixture_path)
    return str(fixture_path)


def test_create_contribs(pubs_parquet, doi_sunet, authors, tmp_path):
    contribs_parquet = tmp_path / "contribs.parquet"
    create_contribs(pubs_parquet, doi_sunet, authors, contribs_parquet)

    df = pl.read_parquet(contribs_parquet)
    assert set(df.columns) == set(["doi", "sunetid", "title", "first_name"]), (
        "columns are correct"
    )

    # first publication got joined to authors
    assert len(df.filter(pl.col("doi") == "0000/abc")) == 1
    row = df.filter(pl.col("doi") == "0000/abc").row(0, named=True)
    assert row["sunetid"] == "user1"
    assert row["first_name"] == "Mary"
    assert row["title"] == "Exquisite article"

    # second publication got joined to authors
    assert len(df.filter(pl.col("doi") == "0000/123")) == 1
    row = df.filter(pl.col("doi") == "0000/123").row(0, named=True)
    assert row["sunetid"] == "user2"
    assert row["first_name"] == "Frederico"
    assert row["title"] == "Fantabulous research"

    # third publication was broken out into two rows since the doi_sunet pickle
    # file indicates it was authored by two people.
    rows = df.filter(pl.col("doi") == "0000/999").sort("sunetid")
    assert len(rows) == 2
    assert rows["sunetid"][0] == "user1"
    assert rows["first_name"][0] == "Mary"
    assert rows["title"][0] == "Perfect prose"

    assert rows["sunetid"][1] == "user2"
    assert rows["first_name"][1] == "Frederico"
    assert rows["title"][1] == "Perfect prose"
