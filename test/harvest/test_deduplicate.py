import logging
import pytest

from rialto_airflow.schema.harvest import Author, Publication
from rialto_airflow.harvest import deduplicate


@pytest.fixture
def dataset(test_session, dim_json, openalex_json, wos_json, sulpub_json, pubmed_json):
    """
    This fixture will create two publications that are duplicates and lack DOIs.
    """
    with test_session.begin() as session:
        pub = Publication(
            doi=None,
            title="My Life",
            apc=123,
            open_access="green",
            pub_year=2024,
            dim_json=dim_json,
            openalex_json=openalex_json,
            wos_json=wos_json,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
        )

        pub2 = Publication(
            doi=None,
            title="My Life",
            apc=123,
            open_access="green",
            pub_year=2024,
            dim_json=dim_json,
            openalex_json=openalex_json,
            wos_json=wos_json,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
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

        pub.authors.append(author1)
        pub2.authors.append(author2)
        session.add(pub)
        session.add(pub2)


def test_wos_deduplicate(test_session, dataset, snapshot, caplog):
    """
    Test that the publication with a duplicate is found and the duplicates removed.
    Authors should be moved to the remaining record.
    """
    caplog.set_level(logging.INFO)
    dupes = deduplicate.remove_wos_duplicates(snapshot)
    assert dupes == 1
    with test_session.begin() as session:
        # only one publication remains and dupe has been deleted
        assert session.query(Publication).count() == 1
        pubs = session.query(Publication).where(
            Publication.wos_json["UID"].astext == "WOS:000123456789"
        )
        assert pubs.count() == 1
        assert len(pubs.one().authors) == 2
        # the second author remains and is linked to one publication
        author2 = session.query(Author).where(Author.orcid == "02980983434").one()
        assert len(author2.publications) == 1
        assert "Found 1 WOS publications with duplicates." in caplog.text
        assert "Deleted 1 publication rows from WOS." in caplog.text


def test_openalex_deduplicate(test_session, dataset, snapshot, caplog):
    """
    Test that the publication with an OpenAlex duplicate is found and the duplicates removed.
    Authors should be moved to the remaining record.
    """
    caplog.set_level(logging.INFO)
    dupes = deduplicate.remove_openalex_duplicates(snapshot)
    assert dupes == 1
    with test_session.begin() as session:
        assert session.query(Publication).count() == 1, (
            "only one publication remains in table"
        )
        pubs = session.query(Publication).where(
            Publication.openalex_json["id"].astext == "https://openalex.org/W123456789"
        )
        assert pubs.count() == 1, "remaining publication has the OpenAlex ID"
        assert len(pubs.one().authors) == 2, "remaining publication has both authors"
        author2 = session.query(Author).where(Author.orcid == "02980983434").one()
        assert len(author2.publications) == 1, (
            "second author exists and is linked to the remaining publication"
        )
        assert "Found 1 OpenAlex publications with duplicates." in caplog.text
        assert "Deleted 1 publication rows from OpenAlex." in caplog.text


def test_merge_pubs(test_session, dataset):
    """
    Test that merge_pubs merges authors and deletes duplicates.
    """
    with test_session.begin() as session:
        pubs = session.query(Publication).all()
        assert len(pubs) == 2, "two pubs exist before merging"
        deleted = deduplicate.merge_pubs(pubs=pubs, session=session)
        assert deleted == 1, "function counts one deletion"
        assert session.query(Publication).count() == 1, "one pub remains"
        remaining_pub = session.query(Publication).one()
        assert len(remaining_pub.authors) == 2, (
            "both authors are linked to the remaining publication"
        )
        author2 = session.query(Author).where(Author.orcid == "02980983434").one()
        assert len(author2.publications) == 1, (
            "second author exists and is linked to the remaining publication"
        )


@pytest.fixture
def dataset2(test_session, dim_json, openalex_json, wos_json, sulpub_json, pubmed_json):
    """
    This fixture will create two publications that are duplicates within different platfordmsand lack DOIs.
    """
    with test_session.begin() as session:
        pub = Publication(
            doi=None,
            title="My Life",
            apc=123,
            open_access="green",
            pub_year=2024,
            dim_json=dim_json,
            openalex_json=openalex_json,
            wos_json=wos_json,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
        )

        pub2 = Publication(
            doi=None,
            title="My Life",
            apc=123,
            open_access="green",
            pub_year=2024,
            dim_json=dim_json,
            openalex_json=None,
            wos_json=wos_json,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
        )

        pub3 = Publication(
            doi=None,
            title="My Life",
            apc=123,
            open_access="green",
            pub_year=2024,
            dim_json=dim_json,
            openalex_json=openalex_json,
            wos_json=None,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
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

        pub.authors.append(author1)
        pub2.authors.append(author2)
        pub3.authors.append(author2)
        session.add(pub)
        session.add(pub2)
        session.add(pub3)


def test_remove_duplicates(test_session, dataset2, snapshot):
    """
    Test that remove_duplicates returns the total number of duplicates removed.
    """
    total_dupes = deduplicate.remove_duplicates(snapshot)
    assert total_dupes == 2, "two duplicates have been removed"
