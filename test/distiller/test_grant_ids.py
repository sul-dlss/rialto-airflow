from rialto_airflow.schema.harvest import Publication
from rialto_airflow.distiller import grant_ids


def test_grants_openalex(openalex_json):
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json=openalex_json,
    )

    assert grant_ids(pub) == "ABI 1661218"
