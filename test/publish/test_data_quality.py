import shutil
from pathlib import Path
from random import randint

import pandas
import pytest
import sqlalchemy
from sqlalchemy import select

from rialto_airflow.publish import data_quality
from rialto_airflow.database import Publication, Author, Funder
from test.utils import TestRow


@pytest.fixture
def dataset(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000001",
            title="My Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json={"type": "article"},
            openalex_json={"type": "preprint"},
            wos_json={
                "static_data": {
                    "fullrecord_metadata": {
                        "normalized_doctypes": {"doctype": ["Article", "Abstract"]}
                    }
                }
            },
        )

        pub.authors.append(
            Author(
                first_name="Jane",
                last_name="Stanford",
                sunet="janes",
                cap_profile_id="1234",
                orcid="0298098343",
                primary_school="School of Humanities and Sciences",
                primary_dept="Social Sciences",
                primary_role="faculty",
                schools=[
                    "Vice Provost for Undergraduate Education",
                    "School of Humanities and Sciences",
                ],
                departments=["Inter-Departmental Programs", "Social Sciences"],
                academic_council=True,
            )
        )

        pub.authors.append(
            Author(
                first_name="Leland",
                last_name="Stanford",
                sunet="lelands",
                cap_profile_id="12345",
                orcid="02980983434",
                primary_school="School of Humanities and Sciences",
                primary_dept="Social Sciences",
                primary_role="staff",
                schools=[
                    "School of Humanities and Sciences",
                ],
                departments=["Social Sciences"],
                academic_council=False,
            )
        )

        pub.authors.append(
            Author(
                first_name="Frederick",
                last_name="Olmstead",
                sunet="folms",
                cap_profile_id="123456",
                orcid="02980983422",
                primary_school="School of Engineering",
                primary_dept="Mechanical Engineering",
                primary_role="faculty",
                schools=["School of Engineering"],
                departments=["Mechanical Engineering"],
                academic_council=False,
            )
        )

        pub.authors.append(
            Author(
                first_name="Frederick",
                last_name="Terman",
                sunet="fterm",
                cap_profile_id="1234567",
                orcid="029809834222",
                primary_school="School of Engineering",
                primary_dept="Electrical Engineering",
                primary_role="faculty",
                schools=["School of Engineering"],
                departments=["Electrical Engineering"],
                academic_council=True,
            )
        )

        pub.funders.append(
            Funder(name="National Institutes of Health", grid_id="12345", federal=True)
        )

        pub.funders.append(
            Funder(name="Andrew Mellon Foundation", grid_id="123456", federal=False)
        )

        session.add(pub)


def test_dataset(test_session, dataset):
    with test_session.begin() as session:
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub
        assert len(pub.authors) == 4
        assert len(pub.funders) == 2


def test_write_authors(test_session, snapshot, dataset, caplog):
    # put the sample data in place
    test_data = Path("test/data")
    shutil.copyfile(
        test_data / "authors-data-quality.csv", snapshot.path / "authors.csv"
    )
    shutil.copyfile(
        test_data / "sulpub-data-quality.jsonl", snapshot.path / "sulpub.jsonl"
    )

    # generate the publications csv file
    csv_path = data_quality.write_authors(snapshot)
    assert csv_path.name == "authors.csv"

    # read it in and make sure it looks right
    df = pandas.read_csv(csv_path)
    assert len(df) == 2243
    assert list(df.columns) == [
        "sunetid",
        "first_name",
        "last_name",
        "full_name",
        "orcidid",
        "orcid_update_scope",
        "cap_profile_id",
        "role",
        "academic_council",
        "primary_affiliation",
        "primary_school",
        "primary_department",
        "primary_division",
        "all_schools",
        "all_departments",
        "all_divisions",
        "active",
        # these are new columns added to existing authors.csv
        "pub_count",
        "new_count",
        "approved_count",
        "denied_count",
        "unknown_count",
    ]

    assert df[df.cap_profile_id == "capid-6831"].pub_count.iloc[0] == 6
    assert df[df.cap_profile_id == "capid-6831"].approved_count.iloc[0] == 6
    assert df[df.cap_profile_id == "capid-6831"].new_count.iloc[0] == 0
    assert df[df.cap_profile_id == "capid-6831"].denied_count.iloc[0] == 0
    assert df[df.cap_profile_id == "capid-6831"].unknown_count.iloc[0] == 0

    assert df[df.cap_profile_id == "capid-48622"].pub_count.iloc[0] == 5
    assert df[df.cap_profile_id == "capid-48622"].approved_count.iloc[0] == 3
    assert df[df.cap_profile_id == "capid-48622"].new_count.iloc[0] == 0
    assert df[df.cap_profile_id == "capid-48622"].denied_count.iloc[0] == 1
    assert df[df.cap_profile_id == "capid-48622"].unknown_count.iloc[0] == 1

    assert df[df.cap_profile_id == "capid-61683"].pub_count.iloc[0] == 1
    assert df[df.cap_profile_id == "capid-61683"].approved_count.iloc[0] == 0
    assert df[df.cap_profile_id == "capid-61683"].new_count.iloc[0] == 1
    assert df[df.cap_profile_id == "capid-61683"].denied_count.iloc[0] == 0
    assert df[df.cap_profile_id == "capid-61683"].unknown_count.iloc[0] == 0

    assert "started writing authors" in caplog.text
    assert "finished writing authors" in caplog.text


def test_write_contributions_by_source(test_session, snapshot, dataset, caplog):
    csv_path = data_quality.write_contributions_by_source(snapshot)
    assert csv_path.is_file()
    assert csv_path.name == "contributions-by-source.csv"

    df = pandas.read_csv(csv_path)
    assert list(df.columns) == [
        "doi",
        "source",
        "present",
        "pub_year",
        "open_access",
        "types",
    ]

    # should be a row per contribution (4) times the number of sources (4)
    assert len(df) == 16

    row = df.iloc[0]
    assert row.doi == "10.000/000001"
    assert row.source == "dim"
    assert bool(row.present) is True
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    row = df.iloc[1]
    assert row.doi == "10.000/000001"
    assert row.source == "openalex"
    assert bool(row.present) is True
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    row = df.iloc[2]
    assert row.doi == "10.000/000001"
    assert row.source == "sulpub"
    assert bool(row.present) is False
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    row = df.iloc[3]
    assert row.doi == "10.000/000001"
    assert row.source == "wos"
    assert bool(row.present) is True
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    # same rows repeat for the second author
    row = df.iloc[4]
    assert row.doi == "10.000/000001"
    assert row.source == "dim"
    assert bool(row.present) is True
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    row = df.iloc[5]
    assert row.doi == "10.000/000001"
    assert row.source == "openalex"
    assert bool(row.present) is True
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    row = df.iloc[6]
    assert row.doi == "10.000/000001"
    assert row.source == "sulpub"
    assert bool(row.present) is False
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    row = df.iloc[7]
    assert row.doi == "10.000/000001"
    assert row.source == "wos"
    assert bool(row.present) is True
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    assert "started writing contributions-by-source.csv" in caplog.text
    assert "finished writing contributions-by-source.csv" in caplog.text


def test_write_sulpub(test_session, snapshot, dataset, caplog):
    # put sample data in place
    test_data = Path("test/data")
    shutil.copyfile(
        test_data / "sulpub-data-quality.jsonl", snapshot.path / "sulpub.jsonl"
    )

    csv_path = data_quality.write_sulpub(snapshot)
    assert csv_path.name == "sulpub.csv"
    assert csv_path.is_file()

    df = pandas.read_csv(csv_path, dtype={"year": str})

    assert len(df) == 1986
    assert list(df.columns) == ["doi", "year", "cap_profile_id", "status", "visibility"]

    pub = df[df["doi"] == "10.1002/ecy.4002"].iloc[0]
    assert pub.year == "2023"
    assert pub.cap_profile_id == "capid-78717|capid-6644|capid-97520"
    assert pub.status == "approved|approved|new"
    assert pub.visibility == "public|public|public"

    assert "started writing sulpub.csv" in caplog.text
    assert "finished writing sulpub.csv" in caplog.text


def test_write_publications(test_session, snapshot, dataset, caplog):
    data_quality.write_publications(snapshot)

    csv_path = data_quality.write_publications(snapshot)
    assert csv_path.is_file()
    assert csv_path.name == "publications.csv"

    df = pandas.read_csv(csv_path)
    assert list(df.columns) == [
        "any_url",
        "any_apc",
        "doi",
        "oa_url",
        "open_access",
        "openalex_apc_list",
        "openalex_apc_paid",
        "pub_year",
        "types",
    ]

    # there's just one publication
    assert len(df) == 1

    row = df.iloc[0]
    assert row.doi == "10.000/000001"
    assert row.pub_year == 2023
    assert row.open_access == "gold"
    assert row.types == "abstract|article|preprint"

    # NOTE: the other columns and their combinations are tested below

    assert "started writing publications.csv" in caplog.text
    assert "finished writing publications.csv" in caplog.text


def test_write_source_counts(test_session, snapshot):
    total = 1000

    # create some random data
    with test_session.begin() as session:
        for i in range(0, total):
            session.add(
                Publication(
                    doi=f"10.000/00000{i}",
                    pub_year=2024,
                    dim_json={"a": "b"} if randint(0, 1) == 1 else None,
                    openalex_json={"a": "b"} if randint(0, 1) == 1 else None,
                    wos_json={"a": "b"},  # all pubs get WoS
                    sulpub_json={"a": "b"} if randint(0, 1) == 1 else None,
                )
            )

    with test_session.begin() as session:
        assert session.query(Publication).count() == total

        csv_path = data_quality.write_source_counts(snapshot)
        assert csv_path.is_file()

        df = pandas.read_csv(csv_path)
        assert len(df) > 0

        source_cols = {
            "Dimensions": "dim_json",
            "Openalex": "openalex_json",
            "PubMed": "pubmed_json",
            "WoS": "wos_json",
        }

        rows = df.to_dict("records")
        for row in rows:
            in_cols = row["sources"].split("|")

            stmt = select(sqlalchemy.func.count()).filter(Publication.pub_year >= 2018)  # type: ignore

            for label in source_cols.keys():
                if label in in_cols:
                    stmt = stmt.filter(
                        getattr(Publication, source_cols[label]).is_not(None)
                    )
                else:
                    stmt = stmt.filter(
                        getattr(Publication, source_cols[label]).is_(None)
                    )

            assert session.execute(stmt).scalars().one() == row["count"]


def test_write_total_source_count(test_session, snapshot):
    # create some random data
    total = 1000

    with test_session.begin() as session:
        for i in range(0, total):
            session.add(
                Publication(
                    doi=f"10.000/00000{i}",
                    pub_year=2024,
                    dim_json={"a": "b"} if i % 4 == 0 else None,  # exactly 25% dim
                    openalex_json={"a": "b"}
                    if i % 2 == 0
                    else None,  # exactly 50% openalex
                    wos_json={"a": "b"},  # all pubs get WoS
                    pubmed_json={"a": "b"}
                    if i % 5 == 0
                    else None,  # exactly 20% pubmed
                )
            )

    with test_session.begin() as session:
        assert session.query(Publication).count() == total

        csv_path = data_quality.write_total_source_count(snapshot)
        assert csv_path.is_file()

        df = pandas.read_csv(csv_path)
        assert len(df) > 0

        assert list(df.columns) == ["source", "total_count"]

        rows = df.to_dict("records")
        assert (rows[0]["source"]) == "Dimensions"
        assert rows[0]["total_count"] == total * 0.25  # 25% for Dimensions
        assert (rows[1]["source"]) == "Openalex"
        assert rows[1]["total_count"] == total * 0.50  # 50% for OpenAlex
        assert (rows[2]["source"]) == "PubMed"
        assert rows[2]["total_count"] == total * 0.20  # 20% for Pubmed
        assert (rows[3]["source"]) == "WoS"
        assert (rows[3]["total_count"]) == total


def test_write_sulpub_source_count(test_session, snapshot):
    total_with_only_sulpub = 100
    total_with_only_wos = 100
    total_with_both_wos_and_sulpub = 100
    total = (
        total_with_only_sulpub + total_with_only_wos + total_with_both_wos_and_sulpub
    )

    # create some sulpub only data
    with test_session.begin() as session:
        for i in range(0, total_with_only_sulpub):
            session.add(
                Publication(
                    doi=f"10.000/00000{i}", pub_year=2024, sulpub_json={"a": "b"}
                )
            )

    # create some wos only data
    with test_session.begin() as session:
        for i in range(0, total_with_only_wos):
            session.add(
                Publication(doi=f"10.000/10000{i}", pub_year=2024, wos_json={"a": "b"})
            )

    # create some sulpub and wos data
    with test_session.begin() as session:
        for i in range(0, total_with_both_wos_and_sulpub):
            session.add(
                Publication(
                    doi=f"10.000/20000{i}",
                    pub_year=2024,
                    sulpub_json={"a": "b"},
                    wos_json={"a": "b"},
                )
            )

    with test_session.begin() as session:
        assert session.query(Publication).count() == total

        csv_path = data_quality.write_sulpub_source_count(snapshot)
        assert csv_path.is_file()

        df = pandas.read_csv(csv_path)
        assert len(df) > 0

        assert list(df.columns) == ["total_count"]

        rows = df.to_dict("records")
        assert rows[0]["total_count"] == total_with_only_sulpub


def test_any_url():
    row = TestRow(
        openalex_json={
            "best_oa_location": {"pdf_url": "https://example.com/article.pdf"}
        }
    )
    assert data_quality._any_url(row) == "https://example.com/article.pdf"

    row = TestRow(
        openalex_json={"open_access": {"oa_url": "https://example.com/article.pdf"}}
    )
    assert data_quality._any_url(row) == "https://example.com/article.pdf"

    row = TestRow(
        openalex_json={
            "primary_location": {"pdf_url": "https://example.com/article.pdf"}
        }
    )
    assert data_quality._any_url(row) == "https://example.com/article.pdf"


def test_any_apc():
    row = TestRow(
        openalex_json={
            "apc_paid": {"value": 9750, "currency": "EUR", "value_usd": 11690}
        }
    )
    assert data_quality._any_apc(row) == 11690

    row = TestRow(
        openalex_json={
            "apc_list": {"value": 9750, "currency": "EUR", "value_usd": 11690}
        }
    )
    assert data_quality._any_apc(row) == 11690


def test_oa_url():
    row = TestRow(
        openalex_json={
            "best_oa_location": {"pdf_url": "https://example.com/article.pdf"}
        }
    )
    assert data_quality._oa_url(row) == "https://example.com/article.pdf"

    row = TestRow(
        openalex_json={"open_access": {"oa_url": "https://example.com/article.pdf"}}
    )
    assert data_quality._oa_url(row) == "https://example.com/article.pdf"


def test_openalex_apc_list():
    row = TestRow(
        openalex_json={
            "apc_list": {"value": 9750, "currency": "EUR", "value_usd": 11690}
        }
    )
    assert data_quality._openalex_apc_list(row) == 11690


def test_openalex_apc_paid():
    row = TestRow(
        openalex_json={
            "apc_paid": {"value": 9750, "currency": "EUR", "value_usd": 11690}
        }
    )
    assert data_quality._openalex_apc_paid(row) == 11690
