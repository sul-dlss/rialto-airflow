import json
import logging
import os
import re
from pathlib import Path

import requests
import xmltodict

from typing import Optional, Dict, Union
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import (
    Author,
    Publication,
    pub_author_association,
)
from rialto_airflow.snapshot import Snapshot
from rialto_airflow.utils import normalize_doi

Params = Dict[str, Union[int, str]]

BASE_URL = "https://eutils.ncbi.nlm.nih.gov"
MAX_RESULTS = 1000  # the maximum number of pubmed IDs we will get for the query
SEARCH_PATH = f"/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&retmax={MAX_RESULTS}"  # this endpoint supports json
FETCH_PATH = f"/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&retmax={MAX_RESULTS}"  # only xml is supported by this endpoint
HEADERS = {"User-Agent": "stanford-library-rialto", "Accept": "application/json"}


def pubmed_key():
    return os.environ.get("AIRFLOW_VAR_PUBMED_KEY")


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
                    logging.warning(f"Reached limit of {limit} publications stopping")
                    break

                pmids = pmids_from_orcid(author.orcid)
                if not pmids:
                    logging.debug(f"No publications found for {author.orcid}")
                    continue

                for pubmed_pub in publications_from_pmids(pmids):
                    count += 1
                    if limit is not None and count > limit:
                        stop = True
                        break

                    doi = normalize_doi(get_doi(pubmed_pub))

                    with get_session(snapshot.database_name).begin() as insert_session:
                        # if there's a DOI constraint violation we need to update instead of insert
                        pub_id = insert_session.execute(
                            insert(Publication)
                            .values(
                                doi=doi,
                                pubmed_json=pubmed_pub,
                            )
                            .on_conflict_do_update(
                                constraint="publication_doi_key",
                                set_=dict(pubmed_json=pubmed_pub),
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

                        jsonl_output.write(json.dumps(pubmed_pub) + "\n")

    return jsonl_file


def fill_in(snapshot: Snapshot):
    """Harvest Pubmed data for DOIs from other publication sources."""
    jsonl_file = snapshot.path / "pubmed.jsonl"
    count = 0
    with jsonl_file.open("a") as jsonl_output:
        with get_session(snapshot.database_name).begin() as select_session:
            stmt = (
                select(Publication.doi)  # type: ignore
                .where(Publication.doi.is_not(None))  # type: ignore
                .where(Publication.pubmed_json.is_(None))
                .execution_options(yield_per=75)
            )

            for rows in select_session.execute(stmt).partitions():
                # use a batch size of 50 DOIs at a time
                dois = [normalize_doi(row.doi) for row in rows]

                logging.debug(f"looking up DOIs {dois}")

                # find PMIDs for the DOIs, and then get full records
                # note that there are likely not as many PMIDs returned as DOIs that were queried
                # and the ordering may be different than the queried DOIs
                # but this doesn't matter, because we will get the full pubmed record for each PMID returned
                # and then find the DOI in the full pubmed record to figure out which publication to update
                # in the database
                pmids = pmids_from_dois(dois)
                pubmed_pubs = publications_from_pmids(pmids)

                for pubmed_pub in pubmed_pubs:
                    doi = normalize_doi(get_doi(pubmed_pub))
                    if doi is None:
                        logging.warning(
                            f"unable to determine what DOI to update for {pubmed_pub}"
                        )
                        continue

                    with get_session(snapshot.database_name).begin() as update_session:
                        update_stmt = (
                            update(Publication)  # type: ignore
                            .where(Publication.doi == doi)
                            .values(pubmed_json=pubmed_pub)
                        )
                        update_session.execute(update_stmt)

                    count += 1
                    jsonl_output.write(json.dumps(pubmed_pub) + "\n")

    logging.info(f"filled in {count} publications")

    return snapshot.path


def pmids_from_orcid(orcid: str) -> list[str]:
    """
    Returns PMIDs associated with a given ORCID.
    """
    # Pubmed doesn't want the full ORCID URIs which are stored in User table
    if m := re.match(r"^https?://orcid.org/(.+)$", orcid):
        orcid = m.group(1)

    return _pubmed_search_api(f"{orcid}[auid]")


# NOTE: You will get a list of PMIDs that may be shorter than the list of DOIs if not all are found
# You cannot assume the ordering is the same as input list of DOIs
def pmids_from_dois(dois: list[str]) -> list[str]:
    """
    Returns PMIDs associated with given list of DOIs.
    """
    if not dois:
        return []

    # Build a batch query using OR operator to search for multiple DOIs at once
    # Format: (doi1[doi] OR doi2[doi] OR doi3[doi])
    doi_terms = [f'"{doi}"[doi]' for doi in dois]
    batch_query = "(" + " OR ".join(doi_terms) + ")"

    # Get all PMIDs for the batch query
    return _pubmed_search_api(batch_query)


def publications_from_pmids(pmids: list[str]) -> list[str]:
    """
    Returns full pubmed records given a list of PMIDs.
    """
    if len(pmids) == 0:
        return []

    query = "id=" + "&id=".join(pmids)

    full_url = f"{BASE_URL}{FETCH_PATH}&api_key={pubmed_key()}"
    logging.debug(f"fetching full records from pubmed with {query}")
    try:
        response = requests.post(full_url, params=query, headers=HEADERS)
        response.raise_for_status()

        results = response.content

        json_results = xmltodict.parse(results)
        pubs = json_results.get("PubmedArticleSet", {}).get("PubmedArticle")
        if not isinstance(pubs, list):
            # if there is only one record, it will not be in a list, but we want to be in one so we can iterate over it
            return [pubs]
        else:
            return pubs
    except requests.exceptions.RequestException as e:  # Catch all requests exceptions
        logging.error(f"Error fetching full pubmed records {query}: {e}")
        return []


def _pubmed_search_api(query) -> list:
    """
    Return a list of pmids given a general search query.
    """

    params: Params = {"term": query}

    full_url = f"{BASE_URL}{SEARCH_PATH}&api_key={pubmed_key()}"
    logging.debug(f"searching pubmed with {params}")
    try:
        response = requests.get(full_url, params=params, headers=HEADERS)
        response.raise_for_status()

        results = response.json()

        if "error" in results:
            logging.error(f"Error in results found for {query}: {results['error']}")
            return []

        if results.get("esearchresult", {}).get("count") is None:
            logging.debug(f"No esearchresult or count found for {query}")
            return []

        if int(results["esearchresult"]["count"]) == 0:
            logging.debug(f"No results found for {query}")
            return []

        return results["esearchresult"]["idlist"]  # return a list of pmids

    except requests.exceptions.RequestException as e:  # Catch all requests exceptions
        logging.error(f"Error searching pubmed query {params}: {e}")
        return []


# get the DOI from the pubmed record
def get_doi(pub) -> Optional[str]:
    # this is the primary way to get the DOI from the pubmed record
    doi = get_identifier(pub, "doi")
    if doi:
        return doi

    # this is a fallback if the DOI is not in the expected place
    try:
        ids = pub["MedlineCitation"]["Article"]["ELocationID"]
        # if there is only one identifier, it is not a list, so make it a list so we can iterate over it
        if not isinstance(ids, list):
            ids = [ids]

        for identifier in ids:
            if "@EIdType" in identifier and identifier["@EIdType"] == "doi":
                return identifier["#text"]
    except KeyError:
        return None

    return None


def get_identifier(pub, identifier_name) -> Optional[str]:
    # look through the list of identifiers in the pubmed record
    try:
        ids = pub["PubmedData"]["ArticleIdList"]["ArticleId"]
        # if there is only one identifier, it is not a list, so make it a list so we can iterate over it
        if not isinstance(ids, list):
            ids = [ids]

        for identifier in ids:
            if "@IdType" in identifier and identifier["@IdType"] == identifier_name:
                return identifier["#text"]
    except KeyError:
        logging.info(f"No identifiers found in pubmed record for {pub}")
        return None

    return None
