from rialto_airflow.distiller.citation_count import citation_count
from rialto_airflow.schema.harvest import Publication


def test_sulpub_fields(test_session, sulpub_json):
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
        assert citation_count(pub_row) is None
