import pytest

from rialto_airflow.publish import publication
from rialto_airflow.database import Publication, Author, Funder
from test.test_utils import TestRow


def dim_json():
    return {"journal": {"title": "Delicious Limes Journal of Science"}}


def openalex_json():
    return {
        "locations": [
            {
                "source": {
                    "type": "journal",
                    "display_name": "Ok Limes Journal of Science",
                }
            },
            {"source": {"type": "geography", "display_name": "New York"}},
        ],
    }


def wos_json():
    return {
        "static_data": {
            "summary": {
                "titles": {
                    "count": 2,
                    "title": [
                        {"type": "source", "content": "Meh Limes Journal of Science"},
                        {"type": "bogus", "content": "Bogus"},
                    ],
                }
            }
        }
    }


def sulpub_json():
    return {"journal": {"name": "Bad Limes Journal of Science"}}


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


def test_journal_with_dimensions_title():
    row = TestRow(
        dim_json=dim_json(),
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    assert publication._journal(row) == "Delicious Limes Journal of Science"


def test_journal_without_dimensions_title():
    row = TestRow(
        openalex_json=openalex_json(),
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    assert publication._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_open_alex_title():
    row = TestRow(
        wos_json=wos_json(),
        sulpub_json=sulpub_json(),
    )
    assert publication._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_open_alex_wos_title():
    row = TestRow(
        sulpub_json=sulpub_json(),
    )
    assert publication._journal(row) == "Bad Limes Journal of Science"
