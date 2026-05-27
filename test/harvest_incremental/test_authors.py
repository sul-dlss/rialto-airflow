import csv
import logging

import pandas
import pytest

from sqlalchemy import update

from rialto_airflow.harvest_incremental.authors import (
    clear_pub_author_links,
    load_authors_table,
)
from rialto_airflow.schema.rialto import (
    Author,
    Harvest,
    Publication,
    pub_author_association,
)

_CSV_FIELDNAMES = [
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


@pytest.fixture
def author(test_incremental_session):
    with test_incremental_session.begin() as session:
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


def test_author_fixture(test_incremental_session, author):
    with test_incremental_session.begin() as session:
        assert session.query(Author).where(Author.sunet == "janes").count() == 1


@pytest.fixture
def authors_csv(tmp_path):
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "sunetid": "janes",
                "first_name": "Jane",
                "last_name": "Stanford",
                "full_name": "Jane Stanford",
                "orcidid": "https://orcid.org/0000-0000-0000-0001",
                "orcid_update_scope": "true",
                "cap_profile_id": "12345",
                "role": "staff",
                "academic_council": "false",
                "primary_affiliation": "Engineering",
                "primary_school": "School of Engineering",
                "primary_department": "Computer Science",
                "primary_division": "Philanthropy Division",
                "all_schools": "Independent Labs, Institutes, and Centers (Dean of Research)|School of Humanities and Sciences",
                "all_departments": "Computer Science|Horticulture",
                "all_divisions": "Philanthropy Division|Other Division",
                "active": "true",
            }
        )
    return fixture_file


def test_load_authors_table(
    test_incremental_session, tmp_path, caplog, authors_csv, mock_rialto_db_name
):
    """
    Make sure we can load the authors.csv.
    """
    load_authors_table(tmp_path)

    with test_incremental_session.begin() as session:
        assert session.query(Author).count() == 1

        author = session.query(Author).where(Author.sunet == "janes").one()
        assert author.cap_profile_id == "12345"
        assert author.first_name == "Jane"
        assert author.last_name == "Stanford"
        assert author.orcid == "https://orcid.org/0000-0000-0000-0001"
        assert author.status
        assert not author.academic_council
        assert author.role == "staff"
        assert author.primary_school == "School of Engineering"
        assert author.primary_dept == "Computer Science"
        assert author.primary_division == "Philanthropy Division"
        assert author.schools == [
            "Independent Labs, Institutes, and Centers (Dean of Research)",
            "School of Humanities and Sciences",
        ]
        assert author.departments == ["Computer Science", "Horticulture"]
        assert author.created_at is not None
        assert author.updated_at is not None

    assert "processed=1 new=1 updated=0 ignored=0" in caplog.text


def test_update_by_sunet(
    test_incremental_session, tmp_path, caplog, authors_csv, mock_rialto_db_name
):
    """
    Make sure that author updates work correctly. Reloading the same data should
    not change the author.updated_at value. Any changes to author's data should
    get loaded and update the author.updated_at timestamp.
    """
    # load initial author
    load_authors_table(tmp_path)
    assert "processed=1 new=1 updated=0 ignored=0" in caplog.text

    # get the author's updated time
    with test_incremental_session.begin() as session:
        authors = session.query(Author).all()
        assert len(authors) == 1
        updated_at = authors[0].updated_at
        assert updated_at is not None

    # loading unchanged data doesn't change updated_at
    load_authors_table(tmp_path)
    with test_incremental_session.begin() as session:
        assert session.query(Author).count() == 1
        author = session.query(Author).where(Author.sunet == "janes").one()
        assert author.updated_at == updated_at, "no changes doesn't alter updated_at"
    assert "processed=1 new=0 updated=0 ignored=1" in caplog.text

    # update the csv with new information for the author
    df = pandas.read_csv(tmp_path / "authors.csv")
    df.at[0, "primary_school"] = "School of Something Else"
    df.at[0, "primary_department"] = "Various"
    df.to_csv(tmp_path / "authors.csv", index=False)

    # load again and ensure values have changed
    load_authors_table(tmp_path)
    assert "processed=1 new=0 updated=1 ignored=0" in caplog.text

    with test_incremental_session.begin() as session:
        assert session.query(Author).count() == 1

        author = session.query(Author).where(Author.sunet == "janes").one()
        assert author.primary_school == "School of Something Else"
        assert author.primary_dept == "Various"
        assert author.updated_at >= updated_at, "updated_at should reflect the change"


def test_load_dupe_orcid(
    test_incremental_session, tmp_path, caplog, authors_csv, mock_rialto_db_name
):
    caplog.set_level(logging.DEBUG)
    # add a row with a duplicate ORCID to the CSV
    with open(authors_csv, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=_CSV_FIELDNAMES)
        writer.writerow(
            {
                "sunetid": "lelands",
                "orcidid": "https://orcid.org/0000-0000-0000-0001",
                "academic_council": True,
                "active": True,
            }
        )

    load_authors_table(tmp_path)
    assert "processed=2 new=1 updated=0 ignored=1" in caplog.text

    with test_incremental_session.begin() as session:
        assert session.query(Author).count() == 1
        assert session.query(Author).where(Author.sunet == "janes").count() == 1
        assert session.query(Author).where(Author.sunet == "lelands").count() == 0

    assert (
        "Ignored author sunet=lelands because there's another author with ORCID https://orcid.org/0000-0000-0000-0001"
        in caplog.text
    )


def test_load_dupe_cap_id(
    test_incremental_session, tmp_path, caplog, authors_csv, mock_rialto_db_name
):

    # create an inactive author linked to a publication in the database
    with test_incremental_session.begin() as session:
        author = Author(
            sunet="lelands",
            cap_profile_id="12345",
            first_name="Leland",
            last_name="Stanford",
            status=False,
        )
        pub = Publication(title="Test pub", authors=[author])
        session.add(pub)
        session.add(author)

    # rewrite the authors.csv with an active author with a different sunet but the same
    # cap_profile_id
    with open(authors_csv, "w", newline="") as csvfile:  # note: ovewriting
        writer = csv.DictWriter(csvfile, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "sunetid": "lelands2",
                "cap_profile_id": "12345",
                "active": True,
                "academic_council": False,
            }
        )

    # load the CSV
    load_authors_table(tmp_path)
    assert "processed=1 new=0 updated=1 ignored=0" in caplog.text

    assert (
        "Updated author sunet=lelands with author sunet=lelands2 for cap_profile_id=12345"
        in caplog.text
    )

    # the old sunet should be gone but the publications should be
    # preserved on the new sunet
    with test_incremental_session.begin() as session:
        assert session.query(Author).count() == 1
        author = session.query(Author).where(Author.sunet == "lelands2").first()
        assert author, "found new author"
        assert author.status is True, "they are active"
        assert len(author.publications) == 1, (
            "removal of old author preserved their publications"
        )


def test_load_null_cap_id(
    test_incremental_session, tmp_path, caplog, authors_csv, mock_rialto_db_name
):
    """
    Test that we can load authors with no cap_profile_id.
    """
    with open(authors_csv, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=_CSV_FIELDNAMES)
        writer.writerow(
            {
                "sunetid": "lelands",
                "cap_profile_id": None,
                "active": False,
                "academic_council": False,
            }
        )

    load_authors_table(tmp_path)
    assert "processed=2 new=2 updated=0 ignored=0" in caplog.text

    with test_incremental_session.begin() as session:
        assert session.query(Author).count() == 2
        assert (
            session.query(Author)
            .where(Author.sunet == "lelands")
            .first()
            .cap_profile_id
            is None
        )


def test_clear_pub_author_links(
    test_incremental_session,
    dataset_incremental,
    mock_rialto_db_name,
    active_harvest_id,
):
    """
    Links between publications and authors should be preserved when doing
    incremental harvests (Harvest.is_full=False)
    """
    with test_incremental_session.begin() as session:
        link_count = session.query(pub_author_association).count()
        assert link_count == 2, "there are pub/author links"

    clear_pub_author_links(active_harvest_id)

    with test_incremental_session.begin() as session:
        link_count = session.query(pub_author_association).count()
        assert link_count == 2, "there still are pub/author links"


def test_clear_pub_author_links_full_harvest(
    test_incremental_session,
    dataset_incremental,
    mock_rialto_db_name,
    active_harvest_id,
):
    """
    Links between publications and authors should be removed when doing
    incremental harvests (Harvest.is_full=True)
    """
    with test_incremental_session.begin() as session:
        link_count = session.query(pub_author_association).count()
        assert link_count == 2, "there are pub/author links"

        session.execute(
            update(Harvest).where(Harvest.id == active_harvest_id).values(is_full=True)
        )

    clear_pub_author_links(active_harvest_id)

    harvest = Harvest.get_by_id(active_harvest_id)
    assert harvest.is_full is True

    with test_incremental_session.begin() as session:
        link_count = session.query(pub_author_association).count()
        assert link_count == 0, "there are no pub/author links"
