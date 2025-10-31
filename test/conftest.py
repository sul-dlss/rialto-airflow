import pytest
from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy_utils import create_database, database_exists, drop_database

from rialto_airflow.database import create_schema, engine_setup
from rialto_airflow.schema.harvest import (
    HarvestSchemaBase,
    Author,
    Funder,
    Publication,
    pub_author_association,
)
from rialto_airflow.schema.reports import ReportsSchemaBase
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.publish import publication


@pytest.fixture
def test_engine(monkeypatch):
    """
    This pytest fixture will ensure that the rialto_test database exists and has
    the database schema configured. If the database exists it will be dropped
    and readded.
    """
    db_host = "postgresql+psycopg2://airflow:airflow@localhost:5432"
    monkeypatch.setenv("AIRFLOW_VAR_RIALTO_POSTGRES", db_host)

    db_name = "rialto_test"
    db_uri = f"{db_host}/{db_name}"

    if database_exists(db_uri):
        drop_database(db_uri)

    create_database(db_uri)

    # note: rialto_airflow.database.create_schema wants the database name not uri
    create_schema(db_name, HarvestSchemaBase)

    # it's handy seeing SQL statements in the log when testing
    return engine_setup(db_name, echo=True)


@pytest.fixture
def test_conn(test_engine):
    """
    Returns a sqlalchemy connection for the test database.
    """
    return test_engine.connect()


@pytest.fixture
def test_session(test_engine):
    """
    Returns a sqlalchemy session for the test database.
    """
    try:
        yield sessionmaker(test_engine)
    finally:
        close_all_sessions()


@pytest.fixture
def mock_authors(test_session):
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Author(
                    sunet="janes",
                    cap_profile_id="12345",
                    orcid="https://orcid.org/0000-0000-0000-0001",
                    first_name="Jane",
                    last_name="Stanford",
                    status=True,
                ),
                Author(
                    sunet="lelands",
                    cap_profile_id="123456",
                    orcid="https://orcid.org/0000-0000-0000-0002",
                    first_name="Leland",
                    last_name="Stanford",
                    status=True,
                ),
                # this user intentionally lacks an ORCID to help test code that
                # ignores harvesting for users that lack an ORCID
                Author(
                    sunet="lelelandjr",
                    cap_profile_id="1234567",
                    orcid=None,
                    first_name="Leland",
                    last_name="Stanford",
                    status=True,
                ),
            ]
        )


@pytest.fixture
def mock_publication(test_session):
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            sulpub_json={"sulpub": "data"},
            wos_json={"wos": "data"},
            dim_json={"dimensions": "data"},
        )
        session.add(pub)
        return pub


@pytest.fixture
def mock_association(test_session, mock_publication, mock_authors):
    with test_session.begin() as session:
        session.execute(
            insert(pub_author_association).values(
                # TODO: should the IDs be looked up in case they aren't always 1?
                publication_id=1,
                author_id=1,
            )
        )


@pytest.fixture
def snapshot(tmp_path):
    return Snapshot.create(data_dir=tmp_path, database_name="rialto_test")


@pytest.fixture
def test_reports_engine(monkeypatch):
    """
    This pytest fixture will ensure that the rialto_reports_test database exists and has
    the database schema configured. If the database exists it will be dropped
    and readded.
    """
    db_host = "postgresql+psycopg2://airflow:airflow@localhost:5432"
    monkeypatch.setenv("AIRFLOW_VAR_RIALTO_POSTGRES", db_host)

    db_name = "rialto_reports_test"
    db_uri = f"{db_host}/{db_name}"

    if database_exists(db_uri):
        drop_database(db_uri)

    create_database(db_uri)

    # note: rialto_airflow.database.create_schema wants the database name not uri
    create_schema(db_name, ReportsSchemaBase)

    # it's handy seeing SQL statements in the log when testing
    return engine_setup(db_name, echo=True)


@pytest.fixture
def test_reports_session(test_reports_engine, monkeypatch):
    """
    Returns a sqlalchemy session for the test database.
    """
    monkeypatch.setattr(publication, "RIALTO_REPORTS_DB_NAME", "rialto_reports_test")
    try:
        yield sessionmaker(engine_setup("rialto_reports_test", echo=True))
    finally:
        close_all_sessions()


@pytest.fixture
def dim_json():
    return {
        "type": "article",
        "doi": "10.000/000001",
        "journal": {"title": "Delicious Limes Journal of Science"},
        "issue": "12",
        "pages": "1-10",
        "volume": "1",
        "mesh_terms": ["Delicions", "Limes"],
        "pmid": "123",
        "linkout": "https://example_dim.com",
        "abstract": "This is a sample Dimensions abstract.",
    }


@pytest.fixture
def dim_json_no_title():
    return {"other_data": {"bogus": "no journal title here"}}


@pytest.fixture
def openalex_json():
    return {
        "id": "https://openalex.org/W123456789",
        "type": "preprint",
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
        "primary_location": {
            "source": {
                "type": "journal",
                "display_name": "Ok Limes Journal of Science",
                "host_organization_name": "Science Publisher Inc.",
                "issn_l": "0009-4978",
                "issn": ["0009-4978", "1523-8253", "1943-5975"],
            }
        },
        "locations": [
            {
                "is_oa": True,
                "landing_page_url": "https://doi.org/10.1073/pnas.17.6.401",
                "pdf_url": "https://example_openalex_pdf.com",
                "source": {
                    "id": "https://openalex.org/S125754415",
                    "display_name": "Proceedings of the National Academy of Sciences of the United States of America",
                    "issn_l": "0027-8424",
                    "issn": ["1091-6490", "0027-8424"],
                    "host_organization": "https://openalex.org/P4310320052",
                    "type": "journal",
                },
                "license": True,
                "version": "publishedVersion",
            },
            {
                "is_oa": True,
                "landing_page_url": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1076072",
                "pdf_url": None,
                "source": {
                    "id": "https://openalex.org/S2764455111",
                    "display_name": "PubMed Central",
                    "issn_l": None,
                    "issn": None,
                    "host_organization": "https://openalex.org/I1299303238",
                    "type": "repository",
                },
                "license": None,
                "version": "publishedVersion",
            },
        ],
        "mesh": [
            {"descriptor_name": "Ok"},
            {"descriptor_name": "Lemons"},
        ],
        "ids": {
            "doi": "10.000/000001",
            "pmid": "1234",
        },
        "abstract_inverted_index": {
            "This": [0],
            "is": [1, 5],
            "an": [2],
            "abstract": [3],
            "which": [
                4,
            ],
            "inverted.": [6],
        },
    }


@pytest.fixture
def openalex_no_title_json():
    return {
        "locations": [
            {
                "bogus": {
                    "nope": "not here",
                }
            },
            {"source": {"type": "geography", "display_name": "New York"}},
        ],
    }


@pytest.fixture
def wos_json():
    return {
        "UID": "WOS:000123456789",
        "fullrecord_metadata": {
            "normalized_doctypes": {"doctype": ["Article", "Abstract"]}
        },
        "static_data": {
            "summary": {
                "pub_info": {
                    "pubyear": "2023",
                    "issue": "10",
                    "vol": "3",
                    "page": {"begin": "1", "end": "8"},
                },
                "titles": {
                    "count": 2,
                    "title": [
                        {"type": "source", "content": "Meh Limes Journal of Science"},
                        {"type": "bogus", "content": "Bogus"},
                    ],
                },
            },
            "fullrecord_metadata": {
                "normalized_doctypes": {"doctype": ["article", "review"]}
            },
        },
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {
                    "identifier": [
                        {
                            "type": "pmid",
                            "value": "1234567",
                        }
                    ],
                },
            }
        },
    }


@pytest.fixture
def wos_json_no_page_info():
    return {
        "static_data": {
            "summary": {
                "pub_info": {
                    "page": {"page_count": "7"},
                },
            }
        }
    }


@pytest.fixture
def sulpub_json():
    return {
        "journal": {
            "name": "Bad Limes Journal of Science",
            "issue": "9",
            "pages": "1-7",
            "volume": "4",
        },
        "issn": ["1234-0000"],
        "pmid": "123456",
    }


@pytest.fixture
def pubmed_json():
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Abstract": {
                    "AbstractText": [
                        {
                            "#text": "Comorbid insomnia with obstructive sleep apnea (COMISA) is associated with worse daytime function and more medical/psychiatric comorbidities vs either condition alone.",
                            "@Label": "OBJECTIVE/BACKGROUND",
                        },
                        {
                            "#text": "E2006-G000-304 was a phase 3, one-month polysomnography trial in adults aged ≥55 years with insomnia.",
                        },
                    ]
                },
                "Title": "Example Journal",
            }
        },
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": [
                    {"@IdType": "pubmed", "#text": "36857419"},
                    {"@IdType": "doi", "#text": "10.1182/bloodadvances.2022008893"},
                ]
            },
        },
    }


@pytest.fixture
def crossref_json():
    return {
        "title": ["My Life"],
        "author": [
            {
                "given": "Jane",
                "family": "Stanford",
                "affiliation": [{"name": "Stanford University"}],
            },
            {
                "given": "Leland",
                "family": "Stanford",
                "affiliation": [{"name": "Stanford University"}],
            },
        ],
        "container-title": ["Bad Limes Journal of Science"],
        "issued": {"date-parts": [[2023]]},
        "DOI": "10.000/000001",
        "ISSN": ["1234-5678"],
    }


@pytest.fixture
def dataset(
    test_session,
    dim_json,
    openalex_json,
    wos_json,
    sulpub_json,
    pubmed_json,
    crossref_json,
):
    """
    This fixture will create two publications, four authors, and two funders.
    It is designed to test the various types of reports we want to output, where
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
            dim_json=dim_json,
            openalex_json=openalex_json,
            wos_json=wos_json,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
            crossref_json=crossref_json,
            types=["article", "preprint"],
            publisher="Science Publisher Inc.",
            academic_council_authored=True,
            faculty_authored=True,
        )

        pub2 = Publication(
            doi="10.000/000002",
            title="My Life Part 2",
            apc=500,
            open_access="green",
            pub_year=2024,
            dim_json=dim_json,
            openalex_json=None,
            wos_json=wos_json,
            sulpub_json=sulpub_json,
            pubmed_json=pubmed_json,
            types=["article", "preprint"],
            academic_council_authored=True,
            faculty_authored=True,
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
