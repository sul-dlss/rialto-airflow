from rialto_airflow.distiller.pages import pages, _openalex_pages
from rialto_airflow.schema.harvest import Publication


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
        assert pages(pub_row) == "1-9"


def test_openalex_pages_start_only():
    openalex_json = {
        "biblio": {"issue": "11", "first_page": "1", "volume": "2"},
    }
    pages = _openalex_pages(openalex_json)
    assert pages == "1"


def test_openalex_pages_end_only():
    openalex_json = {
        "biblio": {"issue": "11", "last_page": "9", "volume": "2"},
    }
    pages = _openalex_pages(openalex_json)
    assert pages == "9"


def test_sulpub_fields(test_session, sulpub_json):
    # Add a publication with fields sourced from sulpub
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/sulpub",
            title="My Sulpub Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json=None,
            wos_json=None,
            sulpub_json=sulpub_json,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
        session.add(pub)

        pub_row = session.query(Publication).filter_by(doi="10.000/sulpub").first()
        assert pages(pub_row) == "1-7"


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
    assert pages(row) == "1-10"
