import csv
import logging

import requests


SUL_PUB_FIELDS = [
    "authorship",
    "title",
    "author",
    "year",
    "type",
    "journal",
    "provenance",
    "doi",
    "sulpubid",
    "date",
    "country",
    "booktitle",
]


def sul_pub_csv(csv_file, host, key, since=None, limit=None):
    with open(csv_file, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=SUL_PUB_FIELDS)
        writer.writeheader()
        for row in harvest(host, key, since, limit):
            writer.writerow(row)


def harvest(host, key, since, limit):
    url = f"https://{host}/publications.json"

    http_headers = {"CAPKEY": key}

    params = {"per": 1000}
    if since:
        params["changedSince"] = since.strftime("%Y-%m-%d")

    page = 0
    record_count = 0
    more = True

    while more:
        page += 1
        params["page"] = page

        logging.info(f"fetching sul_pub results {url} {params}")
        resp = requests.get(url, params=params, headers=http_headers)
        resp.raise_for_status()

        records = resp.json()["records"]
        if len(records) == 0:
            more = False

        for record in records:
            if not approved(record):
                continue

            record_count += 1
            if limit is not None and record_count > limit:
                logging.info(f"stopping with limit={limit}")
                more = False
                break

            pub = {key: record[key] for key in record if key in SUL_PUB_FIELDS}
            pub["doi"] = extract_doi(record)

            yield pub


def extract_doi(record):
    for id in record.get("identifier"):
        if id.get("type") == "doi" and "id" in id:
            return id["id"].replace("https://doi.org/", "")
    return None


def approved(pub):
    """
    Returns True if at least one author has approved the publication, and False if not.
    """
    for authorship in pub["authorship"]:
        if authorship["status"] == "approved":
            return True
    return False
