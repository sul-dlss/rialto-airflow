import pandas
import pytest

from rialto_airflow.publish import publication
from rialto_airflow.database import Publication, Author, Funder
import test.publish.data as test_data


@pytest.fixture
def dataset(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000001",
            title="My Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=test_data.dim_json(),
            openalex_json=test_data.openalex_json(),
            wos_json=test_data.wos_json(),
            sulpub_json=test_data.sulpub_json(),
            pubmed_json=test_data.pubmed_json(),
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


def test_write_publications(test_session, snapshot, dataset, caplog):
    # generate the publications csv file
    csv_path = publication.write_publications(snapshot)

    # read it in and make sure it looks right
    df = pandas.read_csv(csv_path)
    assert len(df) == 1
    row = df.iloc[0]
    assert row.doi == "10.000/000001"
    assert row.pub_year == 2023
    assert row.apc == 123
    assert row.open_access == "gold"
    assert row.types == "article|preprint"
    assert bool(row.federally_funded) is True  # pandas makes federal a numpy.bool_
    assert bool(row.academic_council_authored) is True
    assert bool(row.faculty_authored) is True

    assert "started writing publications" in caplog.text
    assert "finished writing publications" in caplog.text


def test_write_contributions_by_school(test_session, snapshot, dataset, caplog):
    # generate the publications csv file
    csv_path = publication.write_contributions_by_school(snapshot)

    # read it in and make sure it looks right
    df = pandas.read_csv(csv_path)
    assert len(df) == 4

    # sort it so we know what is in each row
    df = df.sort_values(["primary_school", "primary_department"])

    row = df.iloc[0]
    assert bool(row.academic_council_authored) is True
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "faculty"
    assert row.sunet == "fterm"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is True
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Engineering"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    row = df.iloc[1]
    assert bool(row.academic_council_authored) is False
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "faculty"
    assert row.sunet == "folms"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is True
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Engineering"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    row = df.iloc[2]
    assert bool(row.academic_council_authored) is True
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "faculty"
    assert row.sunet == "janes"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is True
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Humanities and Sciences"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    row = df.iloc[3]
    assert bool(row.academic_council_authored) is False
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "staff"
    assert row.sunet == "lelands"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is False
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Humanities and Sciences"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    assert "starting to write contributions by school" in caplog.text
    assert "finished writing contributions by school" in caplog.text


def test_write_contributions_by_department(test_session, snapshot, dataset, caplog):
    # generate the publications csv file
    csv_path = publication.write_contributions_by_department(snapshot)

    # read it in and make sure it looks right
    df = pandas.read_csv(csv_path)
    assert len(df) == 4

    # sort it so we know what is in each row
    df = df.sort_values(["primary_school", "primary_department"])

    row = df.iloc[0]
    assert bool(row.academic_council_authored) is True
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "faculty"
    assert row.sunet == "fterm"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is True
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Engineering"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    row = df.iloc[1]
    assert bool(row.academic_council_authored) is False
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "faculty"
    assert row.sunet == "folms"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is True
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Engineering"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    row = df.iloc[2]
    assert bool(row.academic_council_authored) is False
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "staff"
    assert row.sunet == "lelands"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is False
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Humanities and Sciences"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    row = df.iloc[3]
    assert bool(row.academic_council_authored) is True
    assert row.journal == "Delicious Limes Journal of Science"
    assert row.issue == 12
    assert row.pages == "1-10"
    assert row.volume == 1
    assert row.pmid == 36857419
    assert row.mesh == "Delicions|Limes"
    assert row.url == "https://example_dim.com"
    assert row.title == "My Life"
    assert row.role == "faculty"
    assert row.sunet == "janes"
    assert row.apc == 123
    assert row.doi == "10.000/000001"
    assert bool(row.faculty_authored) is True
    assert bool(row.federally_funded) is True
    assert row.open_access == "gold"
    assert row.primary_school == "School of Humanities and Sciences"
    assert row.pub_year == 2023
    assert row.types == "article|preprint"

    assert "starting to write contributions by school/department" in caplog.text
    assert "finished writing contributions by school/department" in caplog.text
