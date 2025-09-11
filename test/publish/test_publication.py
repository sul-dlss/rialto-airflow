import pytest

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions

import test.publish.data as test_data
from rialto_airflow.database import engine_setup
from rialto_airflow.schema.harvest import (
    Author,
    Funder,
    Publication,
)
from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import (
    Publications,
)


@pytest.fixture
def dataset(test_session):
    """
    This fixture will create two publications, four authors, and two funders.
    It is designed to test the various types of files we want to output, where
    sometimes we want all the publications, and others we want the unique
    publications by school and department.

    The first publication is authored by all 4 authors, and funded by both
    funders. The second publication is authored by the first author, and funded by
    the first funder.

    The first two authors are from the Department of Social Sciences in the
    School of Humanities and Sciences. The last two authors are both from the
    School of Engineering, but one is in the Department of Mechanical Engineering,
    and the other is in Electric Enginering.
    """

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

        pub2 = Publication(
            doi="10.000/000002",
            title="My Life Part 2",
            apc=500,
            open_access="green",
            pub_year=2024,
            dim_json=test_data.dim_json(),
            openalex_json=test_data.openalex_json(),
            wos_json=test_data.wos_json(),
            sulpub_json=test_data.sulpub_json(),
            pubmed_json=test_data.pubmed_json(),
        )

        author1 = Author(
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

        author2 = Author(
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

        author3 = Author(
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

        author4 = Author(
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

        funder1 = Funder(
            name="National Institutes of Health", grid_id="12345", federal=True
        )
        funder2 = Funder(
            name="Andrew Mellon Foundation", grid_id="123456", federal=False
        )

        pub.authors.append(author1)
        pub.authors.append(author2)
        pub.authors.append(author3)
        pub.authors.append(author4)
        pub.funders.append(funder1)
        pub.funders.append(funder2)

        pub2.authors.append(author1)
        pub2.funders.append(funder1)

        session.add(pub)
        session.add(pub2)


@pytest.fixture
def rialto_reports_db_name(monkeypatch):
    monkeypatch.setattr(publication, "RIALTO_REPORTS_DB_NAME", "rialto_reports_test")


@pytest.fixture
def test_reports_session(test_reports_engine):
    """
    Returns a sqlalchemy session for the test database.
    """
    try:
        yield sessionmaker(engine_setup("rialto_reports_test", echo=True))
    finally:
        close_all_sessions()


def test_dataset(test_session, dataset):
    with test_session.begin() as session:
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        pub2 = (
            session.query(Publication).where(Publication.doi == "10.000/000002").first()
        )
        assert pub.title == "My Life"
        assert pub2.title == "My Life Part 2"
        assert len(pub.authors) == 4
        assert len(pub.funders) == 2


def test_export_publications(
    test_reports_session, snapshot, dataset, caplog, rialto_reports_db_name
):
    # generate the publications table
    result = publication.export_publications(snapshot)

    assert result

    with test_reports_session.begin() as session:
        assert session.query(Publications).count() == 2

    with test_reports_session.begin() as session:
        q = session.query(Publications).where(Publications.doi == "10.000/000001")
        db_rows = list(q.all())
        assert len(db_rows) == 1
        assert db_rows[0].apc == 123
        assert db_rows[0].types == "article|preprint"
        assert db_rows[0].open_access == "gold"

    with test_reports_session.begin() as session:
        q = session.query(Publications).where(Publications.doi == "10.000/000002")
        db_rows = list(q.all())
        assert len(db_rows) == 1
        assert db_rows[0].apc == 500
        assert db_rows[0].types == "article|preprint"
        assert db_rows[0].open_access == "green"

    assert "started writing publications table" in caplog.text
    assert "finished writing publications table" in caplog.text
