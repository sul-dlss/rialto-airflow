import csv
import pytest
from rialto_airflow.schema.harvest import Author
from rialto_airflow.harvest.authors import load_authors_table


@pytest.fixture
def author(test_session):
    with test_session.begin() as session:
        session.add(
            Author(
                sunet="janes",
                cap_profile_id="12345",
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
def authors_csv(snapshot):
    # Create a fixture authors CSV file
    fixture_file = snapshot.path / "authors.csv"
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


def test_load_authors_table(test_session, tmp_path, caplog, authors_csv, snapshot):
    load_authors_table(snapshot)

    with test_session.begin() as session:
        assert session.query(Author).count() == 1

        author = session.query(Author).where(Author.sunet == "janes").one()
        assert author.cap_profile_id == "12345"
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
    assert "Errors:" not in caplog.text


def test_load_dupe_orcid(test_session, tmp_path, caplog, authors_csv, snapshot):
    # add a row with a duplicate ORCID to the CSV
    with open(authors_csv, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "lelands",
                "Leland",
                "Stanford, Jr.",
                "Leland Stanford, Jr.",
                "https://orcid.org/0000-0000-0000-0001",
                "true",
                "67890",
                "faculty",
                "true",
                "Humanities",
                "School of Humanities and Sciences",
                "History",
                "History Division",
                "School of Humanities and Sciences",
                "History",
                "History Division",
                "true",
            ]
        )

    load_authors_table(snapshot)

    with test_session.begin() as session:
        assert session.query(Author).count() == 1
        assert session.query(Author).where(Author.sunet == "janes").count() == 1
        assert session.query(Author).where(Author.sunet == "lelands").count() == 0

    assert (
        len([record for record in caplog.records if record.levelname == "WARNING"]) == 2
    )
    for record in caplog.records:
        assert "Skipping author: ('lelands'" in caplog.text
        assert "Errors with 1 author" in caplog.text


def test_load_null_cap_id(test_session, tmp_path, caplog, authors_csv, snapshot):
    # add row with null cap_profile_id
    with open(authors_csv, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(
            [
                [
                    "lelands",
                    "Leland",
                    "Stanford, Jr.",
                    "Leland Stanford, Jr.",
                    "https://orcid.org/0000-0000-0000-0002",
                    "true",
                    None,
                    "faculty",
                    "true",
                    "Humanities",
                    "School of Humanities and Sciences",
                    "History",
                    "History Division",
                    "School of Humanities and Sciences",
                    "History",
                    "History Division",
                    "true",
                ],
                [
                    "rsmith",
                    "Robert",
                    "Smith",
                    "Robert Smith",
                    "https://orcid.org/0000-0000-0000-0003",
                    "true",
                    None,
                    "faculty",
                    "true",
                    "Humanities",
                    "School of Humanities and Sciences",
                    "History",
                    "History Division",
                    "School of Humanities and Sciences",
                    "History",
                    "History Division",
                    "true",
                ],
            ]
        )

    load_authors_table(snapshot)

    with test_session.begin() as session:
        assert session.query(Author).count() == 3
        assert (
            session.query(Author)
            .where(Author.sunet == "lelands")
            .first()
            .cap_profile_id
            is None
        )
        assert (
            session.query(Author).where(Author.sunet == "rsmith").first().cap_profile_id
            is None
        )
