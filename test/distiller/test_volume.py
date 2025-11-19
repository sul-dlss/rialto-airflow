from rialto_airflow.distiller import volume
from rialto_airflow.schema.harvest import Publication


def test_volume():
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        openalex_json={"biblio": {"volume": "1"}},
        dim_json={"volume": "3"},
        pubmed_json={
            "MedlineCitation": {
                "Article": {"Journal": {"JournalIssue": {"Volume": "5"}}}
            }
        },
        sulpub_json={"journal": {"volume": "7"}},
    )

    # slowly peel away the platform metadata that's available to confirm that we are
    # matching in the right order, and looking for values correctly

    assert volume(pub) == "1"

    pub.openalex_json = {}
    assert volume(pub) == "3"

    pub.dim_json = {}
    assert volume(pub) == "5"

    pub.pubmed_json = {}
    assert volume(pub) == "7"

    pub.sulpub_json = {}
    assert volume(pub) is None


def test_volume_list():
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        openalex_json={"biblio": {"volume": ["24"]}},
    )

    assert volume(pub) == "24"
