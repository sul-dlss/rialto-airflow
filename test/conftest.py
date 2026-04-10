import datetime
import logging

import dotenv
import pytest
from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy_utils import create_database, database_exists, drop_database

# load this early so we can rewire the database name to the test database name
import rialto_airflow.schema.rialto

rialto_airflow.schema.rialto.RIALTO_DB_NAME = "rialto_incremental_test"

from rialto_airflow import cli  # noqa: E402
from rialto_airflow.database import create_schema, engine_setup  # noqa: E402
from rialto_airflow.publish import publication  # noqa: E402
from rialto_airflow.schema import rialto as rialto_schema  # noqa: E402
from rialto_airflow.schema import reports as reports_schema  # noqa: E402

dotenv.load_dotenv()

logging.basicConfig(filename="test.log", level=logging.DEBUG)


@pytest.fixture
def mock_rialto_db_name(monkeypatch):
    # TODO: hopefully these can go away once incremental harvesting replaces the
    # full snapshot harvesting, and we don't need to juggle the databases
    monkeypatch.setattr(publication, "RIALTO_DB_NAME", "rialto_incremental_test")
    monkeypatch.setattr(cli, "RIALTO_DB_NAME", "rialto_incremental_test")

    return "rialto_incremental_test"


@pytest.fixture
def mock_incremental_authors(test_incremental_session):
    with test_incremental_session.begin() as session:
        session.bulk_save_objects(
            [
                rialto_schema.Author(
                    sunet="janes",
                    cap_profile_id="12345",
                    orcid="https://orcid.org/0000-0000-0000-0001",
                    first_name="Jane",
                    last_name="Stanford",
                    status=True,
                ),
                rialto_schema.Author(
                    sunet="lelands",
                    cap_profile_id="123456",
                    orcid="https://orcid.org/0000-0000-0000-0002",
                    first_name="Leland",
                    last_name="Stanford",
                    status=True,
                ),
                # this user intentionally lacks an ORCID to help test code that
                # ignores harvesting for users that lack an ORCID
                rialto_schema.Author(
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
def mock_incremental_publication(test_incremental_session):
    with test_incremental_session.begin() as session:
        pub = rialto_schema.Publication(
            doi="10.1515/9781503624153",
            sulpub_json={"sulpub": "data"},
            wos_json={"wos": "data"},
            dim_json={"dimensions": "data"},
        )
        session.add(pub)
        return pub


@pytest.fixture
def mock_incremental_association(
    test_incremental_session, mock_incremental_publication, mock_incremental_authors
):
    with test_incremental_session.begin() as session:
        session.execute(
            insert(rialto_schema.pub_author_association).values(
                publication_id=1,
                author_id=1,
            )
        )


@pytest.fixture
def test_incremental_engine(monkeypatch):
    """
    This pytest fixture will ensure that the rialto_incremental_test database exists and has
    the database schema configured. If the database exists it will be dropped
    and readded.
    """
    db_host = "postgresql+psycopg2://airflow:airflow@localhost:5432"
    monkeypatch.setenv("AIRFLOW_VAR_RIALTO_POSTGRES", db_host)

    db_name = "rialto_incremental_test"
    db_uri = f"{db_host}/{db_name}"

    if database_exists(db_uri):
        drop_database(db_uri)

    create_database(db_uri)

    # note: rialto_airflow.database.create_schema wants the database name not uri
    create_schema(db_name, rialto_schema.RialtoSchemaBase)

    # it's handy seeing SQL statements in the log when testing
    return engine_setup(db_name, echo=True)


@pytest.fixture
def test_incremental_session(test_incremental_engine, mock_rialto_db_name):
    """
    Returns a sqlalchemy session for the test database.
    """
    try:
        yield sessionmaker(test_incremental_engine)
    finally:
        close_all_sessions()


@pytest.fixture
def active_harvest_id(test_incremental_session):
    """
    Create a harvest and return its ID for tests that need to look for an
    active, unfinished harvest.
    """
    with test_incremental_session.begin() as session:
        harvest = rialto_schema.Harvest(
            created_at=datetime.datetime(2026, 4, 27, 16, 38, 10),
        )
        session.add(harvest)
        session.flush()

        return harvest.id


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
    create_schema(db_name, reports_schema.ReportsSchemaBase)

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
        "id": "pub.1000000001",
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
        "recent_citations": 50,
        "authors": [
            {
                "first_name": "Jane",
                "last_name": "Stanford",
                "orcid": ["https://orcid.org/0000-0003-1111-2222"],
            },
            {
                "first_name": "Leland",
                "last_name": "Stanford",
                "orcid": ["https://orcid.org/0000-0004-3333-4444"],
            },
        ],
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
        "cited_by_count": 25,
        "apc_paid": {"value_usd": 123},
        "open_access": {"oa_status": "gold"},
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
            },
            "citation_related": {
                "tc_list": {"silo_tc": [{"coll_id": "WOS", "local_count": 100}]},
            },
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
        "sulpubid": "123456",
        "journal": {
            "name": "Bad Limes Journal of Science",
            "issue": "9",
            "pages": "1-7",
            "volume": "4",
            "year": 1999,
        },
        "issn": ["1234-0000"],
        "pmid": "123456",
        "title": "Sometimes limes are ok",
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
                        {"#text": None},
                        {"@Label": "METHODS"},
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
                "orcid": "https://orcid.org/0000-0003-1111-2222",
            },
            {
                "given": "Leland",
                "family": "Stanford",
                "affiliation": [{"name": "Stanford University"}],
                "orcid": "https://orcid.org/0000-0004-3333-4444",
            },
        ],
        "container-title": ["Bad Limes Journal of Science"],
        "issued": {"date-parts": [[2023]]},
        "DOI": "10.000/000001",
        "ISSN": ["1234-5678"],
    }


@pytest.fixture
def dataset_incremental(
    test_incremental_session,
    dim_json,
    openalex_json,
    wos_json,
    sulpub_json,
    pubmed_json,
    crossref_json,
):
    """
    This fixture mirrors 'dataset' but creates data in the rialto incremental DB
    using the rialto schema models.
    """
    with test_incremental_session.begin() as session:
        now = datetime.datetime.now()

        pub = rialto_schema.Publication(
            doi="10.000/000001",
            title="My Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=dim_json,
            dim_harvested=now,
            openalex_json=openalex_json,
            openalex_harvested=now,
            wos_json=wos_json,
            wos_harvested=now,
            sulpub_json=sulpub_json,
            sulpub_harvested=now,
            pubmed_json=pubmed_json,
            pubmed_harvested=now,
            crossref_json=crossref_json,
            types=["Article", "Preprint"],
            publisher="Science Publisher Inc.",
            academic_council_authored=True,
            faculty_authored=True,
            journal_name="Proceedings of the National Academy of Sciences of the United States of America",
        )

        author1 = rialto_schema.Author(
            first_name="Jane",
            last_name="Stanford",
            sunet="janes",
            cap_profile_id="1234",
            orcid="0298098343",
            primary_school="School of Humanities and Sciences",
            primary_dept="Social Sciences",
            role="faculty",
            schools=[
                "Vice Provost for Undergraduate Education",
                "School of Humanities and Sciences",
            ],
            departments=["Inter-Departmental Programs", "Social Sciences"],
            academic_council=True,
        )

        author2 = rialto_schema.Author(
            first_name="Leland",
            last_name="Stanford",
            sunet="lelands",
            cap_profile_id="12345",
            orcid="02980983434",
            primary_school="School of Humanities and Sciences",
            primary_dept="Social Sciences",
            role="staff",
            schools=[
                "School of Humanities and Sciences",
            ],
            departments=["Social Sciences"],
            academic_council=False,
        )

        pub.authors.append(author1)
        pub.authors.append(author2)
        session.add(pub)
