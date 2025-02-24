import csv
from pathlib import Path
import re


def rialto_authors_file(data_dir):
    """Get the path to the rialto-orgs authors.csv"""
    authors_file = Path(data_dir) / "authors.csv"

    if authors_file.is_file():
        return str(authors_file)
    else:
        raise Exception(f"authors file missing at {authors_file}")


def rialto_authors_orcids(rialto_authors_file):
    """Extract the orcidid column from the authors.csv file"""
    orcids = []
    with open(rialto_authors_file, "r") as file:
        reader = csv.reader(file)
        header = next(reader)
        orcidid = header.index("orcidid")
        for row in reader:
            if row[orcidid]:
                orcids.append(row[orcidid])
    return orcids


def invert_dict(dict):
    """
    Inverting the dictionary so that DOI is the common key for all tasks.
    This adds some complexity here but reduces complexity in downstream tasks.
    """
    original_values = []
    for v in dict.values():
        original_values.extend(v)
    original_values = list(set(original_values))

    inverted_dict = {}
    for i in original_values:
        inverted_dict[i] = [k for k, v in dict.items() if i in v]

    return inverted_dict


def normalize_doi(doi):
    doi = doi.strip().lower()
    doi = doi.replace("https://doi.org/", "").replace("https://dx.doi.org/", "")
    doi = re.sub("^doi: ", "", doi)

    return doi
