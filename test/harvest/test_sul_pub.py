import os

import dotenv
import pandas
import pytest

from rialto_airflow.harvest.sul_pub import sul_pub_csv


dotenv.load_dotenv()

sul_pub_host = os.environ.get("AIRFLOW_VAR_SUL_PUB_HOST")
sul_pub_key = os.environ.get("AIRFLOW_VAR_SUL_PUB_KEY")

no_auth = not (sul_pub_host and sul_pub_key)


@pytest.mark.skipif(no_auth, reason="no sul_pub key")
def test_sul_pub_csv(tmpdir):
    csv_file = tmpdir / "sul_pub.csv"
    sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, limit=2000)
    assert csv_file.isfile()

    df = pandas.read_csv(csv_file)
    assert len(df) == 2000
    assert "title" in df.columns

    # there should be some dois in here
    dois = df.doi[df.doi.notna()]
    assert len(dois) > 1, "there should be at least a few DOIs?"
    assert not dois.iloc[0].startswith("http://"), "DOI IDs not URLs"

    # all the publications should be approved by at least one author
    for _, pub in df.iterrows():
        approved = False
        # the value in the authorship column is a serialized Python dictionary
        for authorship in eval(pub["authorship"]):
            if authorship["status"] == "approved":
                approved = True
        assert approved is True, f"sulpubid={pub['sulpubid']} is marked approved"
