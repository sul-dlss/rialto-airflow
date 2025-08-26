import pytest

from rialto_airflow.database import Author, Publication
from rialto_airflow.harvest import deduplicate
import test.publish.data as test_data


@pytest.fixture
def dataset(test_session):
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
            dim_json=test_data.dim_json(),
            openalex_json=test_data.openalex_json(),
            wos_json=test_data.wos_json(),
            sulpub_json=test_data.sulpub_json(),
            pubmed_json=test_data.pubmed_json(),
        )

        pub2 = Publication(
            doi=None,
            title="My Life",
            apc=123,
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

        pub.authors.append(author1)
        pub2.authors.append(author2)
        session.add(pub)
        session.add(pub2)


def test_wos_deduplicate(test_session, dataset, snapshot):
    """
    Test that the publication with a duplicate is found and the duplicates removed.
    Authors should be moved to the remaining record.
    """
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
