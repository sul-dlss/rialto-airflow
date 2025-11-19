from rialto_airflow.distiller import issue
from rialto_airflow.schema.harvest import Publication


def test_issue():
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        openalex_json={"biblio": {"issue": "2"}},
        dim_json={"issue": "4"},
        pubmed_json={
            "MedlineCitation": {
                "Article": {"Journal": {"JournalIssue": {"Issue": "6"}}}
            }
        },
        sulpub_json={"journal": {"issue": "8"}},
    )

    # slowly peel away the platform metadata that's available to confirm that we are
    # matching in the right order, and looking for values correctly

    assert issue(pub) == "2"

    pub.openalex_json = {}
    assert issue(pub) == "4"

    pub.dim_json = {}
    assert issue(pub) == "6"

    pub.pubmed_json = {}
    assert issue(pub) == "8"

    pub.sulpub_json = {}
    assert issue(pub) is None


def test_issue_list():
    pub = Publication(
        doi="10.000/example",
        title="I'm a Book",
        openalex_json={"biblio": {"issue": ["615"]}},
    )

    assert issue(pub) == "615"
