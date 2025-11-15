from rialto_airflow.distiller.abstract import (
    abstract,
    _pubmed_abstract,
    _rebuild_abstract,
)
from rialto_airflow.schema.harvest import Publication


def test_pubmed_abstract(pubmed_json):
    abstract = _pubmed_abstract(pubmed_json)
    assert (
        abstract
        == "Comorbid insomnia with obstructive sleep apnea (COMISA) is associated with worse daytime function and more medical/psychiatric comorbidities vs either condition alone. E2006-G000-304 was a phase 3, one-month polysomnography trial in adults aged ≥55 years with insomnia."
    )


def test_pubmed():
    metadata = {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
                "Abstract": {
                    "AbstractText": [
                        "This is the abstract.",
                        "It provides a summary of the article.",
                    ]
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

    abstract = _pubmed_abstract(metadata)
    assert abstract == "This is the abstract. It provides a summary of the article."


def test_pubmed_fields_no_abstract():
    pubmed_no_abstract = {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
            }
        },
    }

    abstract = _pubmed_abstract(pubmed_no_abstract)
    assert abstract is None


def test_dimensions_fields(test_session, dim_json_fields):
    # Add a publication with fields sourced from Dimensions
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My Dimensions Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=dim_json_fields,
            openalex_json=None,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
            publisher="Dimensions Publisher",
            journal_name="Delicious Limes Journal of Science",
        )
    session.add(pub)

    row = session.query(Publication).filter_by(doi="10.000/000003").first()
    assert abstract(row) == "This is a sample Dimensions abstract."


def test_openalex(test_session, openalex_json):
    # Add a publication with fields only sourced from OpenAlex
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My OpenAlex Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json=openalex_json,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
        session.add(pub)
        pub_row = session.query(Publication).filter_by(doi="10.000/000003").first()
        assert abstract(pub_row) == "This is an abstract which is inverted."


def test_rebuild_empty_abstract():
    openalex_json = {
        "id": "https://openalex.org/W123456789",
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
        "abstract_inverted_index": None,
    }
    abstract = _rebuild_abstract(openalex_json)
    assert abstract is None
