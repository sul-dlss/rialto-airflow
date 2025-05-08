import json
import logging
import os
import re
from pathlib import Path

import requests
import xml.etree.ElementTree as et
import xmltodict

from typing import Optional, Dict, Union

# from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import (
    Author,
    Publication,
    get_session,
    pub_author_association,
)
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi

Params = Dict[str, Union[int, str]]

PUBMED_KEY = os.environ.get("AIRFLOW_VAR_PUBMED_KEY")
BASE_URL = "https://eutils.ncbi.nlm.nih.gov"
MAX_RESULTS = 1000  # the maximum number of pubmed IDs we will get for the query
SEARCH_PATH = f"/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&api_key={PUBMED_KEY}&retmax={MAX_RESULTS}"  # this endpoint suppots json
FETCH_PATH = f"/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&api_key={PUBMED_KEY}&retmax={MAX_RESULTS}"  # only xml is supported by this endpoint
HEADERS = {"User-Agent": "stanford-library-rialto", "Accept": "application/json"}


def harvest(snapshot: Snapshot, limit=None) -> Path:
    """
    Walk through all the Author ORCIDs and generate publications for them from pubmed.
    """
    jsonl_file = snapshot.path / "pubmed.jsonl"
    count = 0
    stop = False

    with jsonl_file.open("w") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            # get all authors that have an ORCID
            for author in (
                select_session.query(Author).where(Author.orcid.is_not(None)).all()  # type: ignore
            ):
                if stop is True:
                    logging.info(f"Reached limit of {limit} publications stopping")
                    break

                pmids = pmids_from_orcid(author.orcid)
                if not pmids:
                    logging.info(f"No publications found for {author.orcid}")
                    continue

                for pubmed_pub in publications_from_pmids(pmids):
                    count += 1
                    if limit is not None and count > limit:
                        stop = True
                        break

                    doi = normalize_doi(get_doi(pubmed_pub))

                    # parse the XML into a dict and then onto a json string
                    # this is because as of 2025, the pubmed API will only return XML
                    pubmed_pub_as_json_string = json.dumps(
                        xmltodict.parse(et.tostring(pubmed_pub))
                    )
                    pubmed_pub_as_json = json.loads(pubmed_pub_as_json_string)

                    with get_session(snapshot.database_name).begin() as insert_session:
                        # if there's a DOI constraint violation we need to update instead of insert
                        pub_id = insert_session.execute(
                            insert(Publication)
                            .values(
                                doi=doi,
                                pubmed_json=pubmed_pub_as_json,
                            )
                            .on_conflict_do_update(
                                constraint="publication_doi_key",
                                set_=dict(pubmed_json=pubmed_pub_as_json),
                            )
                            .returning(Publication.id)
                        ).scalar_one()

                        # a constraint violation is ok here, since it means we
                        # already know that the publication is by the author
                        insert_session.execute(
                            insert(pub_author_association)
                            .values(publication_id=pub_id, author_id=author.id)
                            .on_conflict_do_nothing()
                        )

                        jsonl_output.write(pubmed_pub_as_json_string + "\n")

    return jsonl_file


def pmids_from_orcid(orcid):
    """
    Returns PMIDs associated with a given ORCID.
    """
    # Pubmed doesn't want the full ORCID URIs which are stored in User table
    if m := re.match(r"^https?://orcid.org/(.+)$", orcid):
        orcid = m.group(1)

    return _pubmed_search_api(f"{orcid}[auid]")


def publications_from_pmids(pmids):
    """
    Returns full pubmed records given a list of PMIDs.
    """
    if not pmids:
        return None

    query = "id=" + "&id=".join(pmids)
    return _pubmed_fetch_api(query)


def _pubmed_search_api(query) -> list:
    """
    Return a list of pmids given a general search query.
    """

    params: Params = {"term": query}

    http = requests.Session()

    full_url = f"{BASE_URL}{SEARCH_PATH}"
    logging.info(f"searching pubmed with {params}")
    resp: requests.Response = http.get(full_url, params=params, headers=HEADERS)

    results = resp.json()
    if not results:
        logging.info(f"Empty results found for {query}")
        return []

    if int(results["esearchresult"]["count"]) == 0:
        logging.info(f"No results found for {query}")
        return []

    return results["esearchresult"]["idlist"]  # return a list of pmids


def _pubmed_fetch_api(query):
    """
    Returns full pubmed records given a list of pmids.
    """

    http = requests.Session()

    full_url = f"{BASE_URL}{FETCH_PATH}"
    logging.info(f"fetching full records from pubmed with {query}")
    resp: requests.Response = http.post(full_url, params=query, headers=HEADERS)

    results = resp.content
    if not results:
        logging.info(f"Empty results found for {query}")
        return

    return et.fromstring(results).findall(
        ".//PubmedArticle"
    )  # returned the parsed xml tree of pubmed articles


# get the DOI from the pubmed record
def get_doi(pub) -> Optional[str]:
    # this is the primary way to get the DOI from the pubmed record
    doi = get_identifier(pub, "doi")
    if doi:
        return doi

    # this is a fallback if the DOI is not in the expected place
    alt_doi = pub.findall('.//ELocationID[@EIdType="doi"]')
    if alt_doi:
        return alt_doi[0].text

    return None


def get_identifier(pub, identifier_name) -> Optional[str]:
    # find the specified identifier in the PubmedData section
    identifier_value = pub.findall(
        f'.//PubmedData/ArticleIdList/*[@IdType="{identifier_name}"]'
    )
    if identifier_value:
        return identifier_value[0].text

    return None
