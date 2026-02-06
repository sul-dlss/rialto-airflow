from rialto_airflow.distiller import natural_key
from rialto_airflow.schema.harvest import Publication


def test_natural_key():
    pub = Publication(
        openalex_json={
            "title": "I'm a Book",
            "publication_year": 1880,
            "authorships": [{"author": {"display_name": "Jane Stanford"}}],
        }
    )

    assert natural_key(pub) == "imabook:janestanford:1880"
