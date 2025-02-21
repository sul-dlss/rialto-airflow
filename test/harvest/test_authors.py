import csv
import pytest
from rialto_airflow.database import Author
from rialto_airflow.harvest.authors import load_authors_table


@pytest.fixture
def author(test_session):
    with test_session.begin() as session:
        session.add(
            Author(
                sunet="janes",
                orcid="https://orcid.org/0000-0000-0000-0001",
                first_name="Jane",
                last_name="Stanford",
                status=True,
            )
        )


def test_author_fixture(test_session, author):
    with test_session.begin() as session:
        assert session.query(Author).where(Author.sunet == "janes").count() == 1


@pytest.fixture
def authors_csv(tmp_path):
    # Create a fixture authors CSV file
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
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
            ]
        )
        writer.writerow(
            [
                "janes",
                "Jane",
                "Stanford",
                "Jane Stanford",
                "https://orcid.org/0000-0000-0000-0001",
                "true",
                "12345",
                "staff",
                "false",
                "Engineering",
                "School of Engineering",
                "Computer Science",
                "Philanthropy Division",
                "Independent Labs, Institutes, and Centers (Dean of Research)|School of Humanities and Sciences",
                "Computer Science|Horticulture",
                "Philanthropy Division|Other Division",
                "true",
            ]
        )
    return fixture_file


def test_load_authors_table(test_session, tmp_path, authors_csv):
    harvest_config = {"snapshot_dir": tmp_path, "database_name": "rialto_test"}
    load_authors_table(harvest_config)

    with test_session.begin() as session:
        assert session.query(Author).count() == 1

        author = session.query(Author).where(Author.sunet == "janes").one()
        assert author.first_name == "Jane"
        assert author.last_name == "Stanford"
        assert author.orcid == "https://orcid.org/0000-0000-0000-0001"
        assert author.status
        assert not author.academic_council
        assert author.primary_role == "staff"
        assert author.primary_school == "School of Engineering"
        assert author.primary_dept == "Computer Science"
        assert author.primary_division == "Philanthropy Division"
        assert author.schools == [
            "Independent Labs, Institutes, and Centers (Dean of Research)",
            "School of Humanities and Sciences",
        ]
        assert author.departments == ["Computer Science", "Horticulture"]
        assert author.created_at is not None
