import os

import dotenv

from rialto_airflow.harvest import dimensions

dotenv.load_dotenv()

dimensions_user = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER")
dimensions_password = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS")


def test_publications_from_dois():
    # use batch_size=1 to test paging for two DOIs
    pubs = list(
        dimensions.publications_from_dois(
            ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], batch_size=1
        )
    )
    assert len(pubs) == 2
    assert len(pubs[0].keys()) == 74, "first publication has 74 columns"
    assert len(pubs[1].keys()) == 74, "second publication has 74 columns"


def test_publication_fields():
    fields = dimensions.publication_fields()
    assert len(fields) == 74
    assert "title" in fields
