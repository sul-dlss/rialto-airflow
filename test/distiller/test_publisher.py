import pytest

from rialto_airflow.schema.harvest import Publication, Author
from rialto_airflow.harvest.distill import distill
from rialto_airflow.distiller import publisher, journal_name, journal_issn

# When there is no OpenAlex publisher information we try to look it up in
# OpenAlex using an ISSN found in the other record metadata. This is why both publisher
# journal_name and ISSN are being tested here.


@pytest.fixture
def pubmed_json():
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Article Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
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


def test_openalex_publisher_journal(sulpub_json, dim_json):
    """
    Test that publisher is distilled from OpenAlex JSON first if available.
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,
        dim_json=dim_json,
        openalex_json={
            "title": "On the dangers of stochastic parrots (openalex)",
            "primary_location": {
                "source": {
                    "id": "https://openalex.org/S2764375719",
                    "display_name": "Not the journal name to use",
                    "issn_l": "0009-4978",
                    "issn": ["0009-4978", "1523-8253", "1943-5975"],
                    "host_organization": "https://openalex.org/P4310316146",
                    "host_organization_name": "Some Publisher",
                    "type": "journal",
                }
            },
            "locations": [
                {
                    "id": "doi:10.5860/choice.51-7042",
                    "source": {
                        "id": "https://openalex.org/S2764375719",
                        "display_name": "Real Journal Name",
                        "issn_l": "9999-9999",
                        "type": "journal",
                    },
                },
                {
                    "id": "pmh:oai:archive.org:I",
                    "source": {
                        "id": "https://openalex.org/S4377196541",
                        "display_name": "Internet Archive (Internet Archive)",
                        "issn_l": "8888-8888",
                        "type": "repository",
                    },
                },
            ],
        },
    )

    assert publisher(pub) == "Some Publisher"
    assert journal_name(pub) == "Real Journal Name"


def test_pubmed_publisher_journal(pubmed_json):
    pub = Publication(
        doi="10.1515/9781503624153",
        pubmed_json=pubmed_json,
        dim_json=None,
        openalex_json=None,
    )

    assert journal_issn(pub) == "1873-2054"
    # will do live lookup in OpenAlex Sources API
    assert journal_name(pub) == "Health & Place"
    assert publisher(pub) == "Elsevier"


def test_dimensions_publisher_journal():
    """
    Test that publisher is distilled from Dimensions JSON.
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json=None,
        dim_json={
            "type": "article",
            "issn": "1476-4687",  # Nature
        },
    )

    assert journal_issn(pub) == "1476-4687"
    # will do live lookup in OpenAlex Sources API
    assert publisher(pub) == "Springer Nature"
    assert journal_name(pub) == "Nature"


def test_author_based_fields(test_session, snapshot, openalex_json, dim_json):
    """
    academic_council_authored should be true if any authors are academic council
    """
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            openalex_json=openalex_json,
            dim_json=dim_json,
        )
        pub2 = Publication(
            doi="10.1515/0003",
            openalex_json=openalex_json,
            dim_json=dim_json,
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
        pub.authors.append(author2)
        pub2.authors.append(author2)
        session.add(pub)
        session.add(pub2)

    distill(snapshot)

    academic_pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )
    assert academic_pub.academic_council_authored
    assert academic_pub.faculty_authored

    non_academic_pub = (
        session.query(Publication).where(Publication.doi == "10.1515/0003").first()
    )
    assert non_academic_pub.academic_council_authored is False
    assert non_academic_pub.faculty_authored is False


def test_journal_issn(sulpub_json, dim_json, openalex_json):
    """
    Test that journal ISSN is extracted from multiple sources
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json=sulpub_json,  # 3333-3333
        dim_json=dim_json,  # 1111-1111
        openalex_json=openalex_json,  # 0009-4978, 1523-8253, 1943-5975
        crossref_json={
            "ISSN": ["0000-0000", "1111-1111"],
        },
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "Journal": {
                        "Title": "The Medical Journal",
                        "ISSN": {
                            "#text": "4444-4444",
                            "@IssnType": "Electronic",
                        },
                    },
                }
            },
        },
    )

    # ISSNs extracted from multiple sources
    assert (
        journal_issn(pub)
        == "0000-0000|0009-4978|1111-1111|1523-8253|1943-5975|3333-3333|4444-4444"
    )


def test_null_issn():
    # Add a publication with fields only sourced from OpenAlex
    pub = Publication(
        doi="10.000/some_doi",
        title="My OpenAlex Life",
        apc=123,
        open_access="gold",
        pub_year=2023,
        dim_json=None,
        openalex_json={
            "id": "https://openalex.org/W123456789",
            "biblio": {
                "issue": "11",
                "first_page": "1",
                "last_page": "9",
                "volume": "2",
            },
            "primary_location": {
                "source": {
                    "type": "journal",
                    "display_name": "Ok Limes Journal of Science",
                    "host_organization_name": "Science Publisher Inc.",
                    "issn_l": None,
                    "issn": None,
                }
            },
        },
        wos_json=None,
        sulpub_json=None,
        pubmed_json=None,
        crossref_json=None,
        types=["Article", "Preprint"],
    )

    assert journal_issn(pub) is None


def test_invalid_issn():
    """
    Test that invalid ISSNs are ignored.
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        sulpub_json={
            "journal_issn": "",
        },
        dim_json={
            "issn": 1,
        },
        crossref_json={
            "ISSN": ["", "abcd-efgh", "12345678", "1234-0000"],
        },
    )

    # no valid ISSNs
    assert journal_issn(pub) == "1234-0000"
