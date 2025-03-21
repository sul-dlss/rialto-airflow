import pandas
import pytest

from rialto_airflow.publish import openaccess
from rialto_airflow.database import Publication, Author, Funder


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
        assert len(pub.authors) == 2
        assert len(pub.funders) == 2


def test_write_publications(test_session, snapshot, dataset, caplog):
    # generate the publications csv file
    csv_path = openaccess.write_publications(snapshot)

    # read it in and make sure it looks right
    df = pandas.read_csv(csv_path)
    assert len(df) == 1

    row = df.iloc[0]
    assert row.doi == "10.000/000001"
    assert row.pub_year == 2023
    assert row.apc == 123
    assert row.open_access == "gold"
    assert row.types == "article|preprint"
    assert row.funders == "National Institutes of Health|Andrew Mellon Foundation"
    assert bool(row.federally_funded) is True  # pandas makes federal a numpy.bool_

    assert "started writing publications" in caplog.text
    assert "finished writing publications" in caplog.text


def test_write_contributions(test_session, snapshot, dataset, caplog):
    # generate the publications csv file
    csv_path = openaccess.write_contributions(snapshot)

    # read it in and make sure it looks right
    df = pandas.read_csv(csv_path)
    assert len(df) == 2

    # firt row of contributions.csv should look like this
    row = df.iloc[0]
    assert row.sunet == "janes"
    assert row.role == "faculty"
    assert bool(row.academic_council) is True
    assert row.primary_school == "School of Humanities and Sciences"
    assert row.primary_department == "Social Sciences"
    assert row.doi == "10.000/000001"
    assert row.pub_year == 2023
    assert row.apc == 123
    assert row.open_access == "gold"
    assert row.types == "article|preprint"
    assert bool(row.federally_funded) is True  # pandas makes federal a numpy.bool_

    # second row of contributions.csv should look like this
    row = df.iloc[1]
    assert row.sunet == "lelands"
    assert row.role == "staff"
    assert bool(row.academic_council) is False
    assert row.primary_school == "School of Humanities and Sciences"
    assert row.primary_department == "Social Sciences"
    assert row.doi == "10.000/000001"
    assert row.pub_year == 2023
    assert row.apc == 123
    assert row.open_access == "gold"
    assert row.types == "article|preprint"
    assert bool(row.federally_funded) is True  # pandas makes federal a numpy.bool_

    assert "starting to write contributions" in caplog.text
    assert "finished writing contributions" in caplog.text
