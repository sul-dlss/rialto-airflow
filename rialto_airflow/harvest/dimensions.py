import csv
import logging
import os
import pickle
import time
from functools import cache

import dimcli
import pandas as pd
import requests
from more_itertools import batched

from rialto_airflow.utils import invert_dict


def dois_from_orcid(orcid):
    logging.info(f"looking up dois for orcid {orcid}")
    q = """
        search publications where researchers.orcid_id = "{}"
        return publications [doi]
        limit 1000
        """.format(orcid)

    result = query_with_retry(q, 20)

    if len(result["publications"]) == 1000:
        logging.warning("Truncated results for ORCID %s", orcid)
    for pub in result["publications"]:
        if pub.get("doi"):
            doi_id = pub["doi"].replace("https://doi.org/", "")
            yield doi_id


def doi_orcids_pickle(authors_csv, pickle_file, limit=None) -> None:
    """
    Read the ORCIDs in the provided rialto-orgs authors.csv file and write a
    dictionary mapping of DOI -> [ORCID] to a pickle file at the provided path.
    """
    df = pd.read_csv(authors_csv)
    orcids = df[df["orcidid"].notna()]["orcidid"]
    orcid_dois = {}

    for orcid_url in orcids[:limit]:
        orcid = orcid_url.replace("https://orcid.org/", "")
        dois = list(dois_from_orcid(orcid))
        orcid_dois.update({orcid: dois})

    with open(pickle_file, "wb") as handle:
        pickle.dump(invert_dict(orcid_dois), handle, protocol=pickle.HIGHEST_PROTOCOL)


def publications_csv(dois, csv_file) -> None:
    with open(csv_file, "w") as output:
        writer = csv.DictWriter(output, publication_fields())
        writer.writeheader()
        for pub in publications_from_dois(dois):
            logging.info(f"writing metadata for {pub.get('doi')}")
            writer.writerow(pub)


def publications_from_dois(dois: list, batch_size=200):
    """
    Get the publications metadata for the provided list of DOIs and write as a
    CSV file.
    """
    fields = " + ".join(publication_fields())
    for doi_batch in batched(dois, batch_size):
        doi_list = ",".join(['"{}"'.format(doi) for doi in doi_batch])

        q = f"""
            search publications where doi in [{doi_list}]
            return publications [{fields}]
            limit 1000
            """

        result = query_with_retry(q, retry=5)

        for pub in result["publications"]:
            yield normalize_publication(pub)


@cache
def publication_fields():
    """
    Get a list of all possible fields for publications.
    """
    result = dsl().query("describe schema")
    return list(result.data["sources"]["publications"]["fields"].keys())


def normalize_publication(pub) -> dict:
    for field in publication_fields():
        if field not in pub:
            pub[field] = None

    return pub


@cache  # TODO: maybe the login should expire after some time?
def login():
    """
    Login to Dimensions API and cache the result.
    """
    dimcli.login(
        os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER"),
        os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS"),
        "https://app.dimensions.ai",
    )


def dsl():
    """
    Get the Dimensions DSL for querying.
    """
    login()
    return dimcli.Dsl(verbose=False)


def query_with_retry(q, retry=5):
    # Catch all request exceptions since dimcli currently only catches HTTP exceptions
    # see: https://github.com/digital-science/dimcli/issues/88
    try_count = 0
    while True:
        try_count += 1

        try:
            # dimcli will retry HTTP level errors, but not ones involving the connection
            return dsl().query(q, retry=retry)
        except requests.exceptions.RequestException as e:
            if try_count > retry:
                logging.error(
                    "Dimensions API call %s resulted in error: %s", try_count, e
                )
                raise e
            else:
                logging.warning(
                    "Dimensions query error retry %s of %s: %s", try_count, retry, e
                )
                time.sleep(try_count * 10)
