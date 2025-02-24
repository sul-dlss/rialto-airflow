import csv
import logging
import os
import pickle
import time

from more_itertools import batched
from pyalex import Authors, Works, config, api

from rialto_airflow.utils import invert_dict, normalize_doi, normalize_orcid

config.email = os.environ.get("AIRFLOW_VAR_OPENALEX_EMAIL")
config.max_retries = 5
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


def doi_orcids_pickle(authors_csv, pickle_file, limit=None):
    """
    Pass in the Authors CSV and generate a DOI -> ORCID mapping as a pickle file.
    """
    with open(authors_csv, "r") as csv_input:
        orcid_dois = {}
        count = 0
        for row in csv.DictReader(csv_input):
            orcid = normalize_orcid(row["orcidid"])
            if orcid:
                count += 1
                orcid_dois[orcid] = list(dois_from_orcid(orcid))
                if limit is not None and count > limit:
                    break

    with open(pickle_file, "wb") as handle:
        pickle.dump(invert_dict(orcid_dois), handle, protocol=pickle.HIGHEST_PROTOCOL)


def dois_from_orcid(orcid: str, limit=None):
    """
    Pass in the ORCID ID and get back a list of DOIs for publications authored by that person.
    """

    # TODO: I think we can maybe have this function take a list of orcids and
    # batch process them since we can filter by multiple orcids in one request?

    # TODO: get a key so we don't have to sleep!
    time.sleep(1)

    logging.info(f"looking up dois for orcid {orcid}")

    # get the first (and hopefully only) openalex id for the orcid
    authors = Authors().filter(orcid=orcid).get()
    if len(authors) == 0:
        return []
    elif len(authors) > 1:
        logging.warn(f"found more than one openalex author id for {orcid}")
    author_id = authors[0]["id"]

    # get all the works for the openalex author id
    dois = set()
    for page in (
        Works().filter(author={"id": author_id}).select(["doi"]).paginate(per_page=200)
    ):
        for pub in page:
            if pub.get("doi"):
                dois.add(normalize_doi(pub.get("doi")))
                if limit is not None and len(dois) == limit:
                    return list(dois)

    return list(dois)


def publications_csv(dois: list, csv_file: str) -> None:
    """
    Get publication records for a list of DOIs and create a CSV file.
    """
    with open(csv_file, "w") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        for pub in publications_from_dois(dois):
            writer.writerow(pub)


def publications_from_dois(dois: list):
    """
    Look up works by DOI in batches that fit within OpenAlex request size limits
    """
    for doi_batch in batched(dois, 50):
        # Setting batch size to 50 to avoid 400 errors from OpenAlex API when GET query string is greater than 4096 characters
        # Based on experimentation, 75 is too high. 50 is the default per_page size, so we could consider removing pagination in the future.
        # TODO: do we need this to stay within 100,000 requests / day API quota?
        time.sleep(1)

        doi_list = "|".join([doi for doi in doi_batch])
        try:
            for page in Works().filter(doi=doi_list).paginate(per_page=200):
                for pub in page:
                    yield normalize_publication(pub)
        except api.QueryError:
            # try dois individually
            for doi in doi_batch:
                try:
                    pubs = Works().filter(doi=doi).get()
                    if len(pubs) > 1:
                        logging.warn(f"Found multiple publications for DOI {doi}")
                    if len(pubs) > 0:
                        yield normalize_publication(pubs[0])
                except api.QueryError as e:
                    logging.error(f"OpenAlex QueryError for {doi}: {e}")
                    continue


def normalize_publication(pub) -> dict:
    """
    Ensure missing keys get empty values written to CSV.
    """
    for field in FIELDS:
        if field not in pub:
            pub[field] = None

    return pub


FIELDS = [
    "abstract_inverted_index",
    "abstract_inverted_index_v3",
    "authorships",
    "apc_list",
    "apc_paid",
    "best_oa_location",
    "biblio",
    "citation_normalized_percentile",
    "cited_by_api_url",
    "cited_by_count",
    "cited_by_percentile_year",
    "concepts",
    "corresponding_author_ids",
    "corresponding_institution_ids",
    "countries_distinct_count",
    "counts_by_year",
    "created_date",
    "datasets",
    "display_name",
    "doi",
    "fulltext_origin",
    "fwci",
    "grants",
    "has_fulltext",
    "id",
    "ids",
    "indexed_in",
    "institution_assertions",
    "institutions_distinct_count",
    "is_authors_truncated",
    "is_paratext",
    "is_retracted",
    "keywords",
    "language",
    "license",
    "locations",
    "locations_count",
    "mesh",
    "ngrams_url",
    "open_access",
    "primary_location",
    "primary_topic",
    "publication_date",
    "publication_year",
    "referenced_works",
    "referenced_works_count",
    "related_works",
    "sustainable_development_goals",
    "topics",
    "title",
    "type",
    "type_crossref",
    "updated_date",
    "versions",
]
