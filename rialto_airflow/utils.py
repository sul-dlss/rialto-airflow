import re
from pathlib import Path
from typing import Optional


def rialto_authors_file(data_dir):
    """Get the path to the rialto-orgs authors.csv"""
    authors_file = Path(data_dir) / "authors.csv"

    if authors_file.is_file():
        return str(authors_file)
    else:
        raise Exception(f"authors file missing at {authors_file}")


def rialto_active_authors_file(data_dir):
    """Get the path to the rialto-orgs authors_active.csv"""
    authors_file = Path(data_dir) / "authors_active.csv"

    if authors_file.is_file():
        return str(authors_file)
    else:
        raise Exception(f"authors file missing at {authors_file}")


def normalize_doi(doi):
    if doi is None:
        return None

    doi = (
        doi.lower()
        .replace(" ", "")
        .replace("https://doi.org/", "")
        .replace("https://dx.doi.org/", "")
    )

    doi = re.sub(r"^doi:\s?", "", doi)

    return doi


def normalize_pmid(pmid):
    if pmid is None:
        return None

    pmid = pmid.strip().lower()
    pmid = pmid.replace("https://pubmed.ncbi.nlm.nih.gov/", "").replace("medline:", "")

    return pmid


def normalize_orcid(orcid):
    orcid = orcid.strip().lower()
    orcid = orcid.replace("https://orcid.org/", "").replace(
        "https://sandbox.orcid.org/", ""
    )

    return orcid


def piped(lst: list[str] | None) -> Optional[str]:
    """
    Return a list as pipe delimited or None if None is passed in.
    """
    if lst is None:
        return None

    return "|".join(lst)


def join_keys(d: dict, *keys):
    """
    Join the values in a dictionary, removing any missing values. It is assumed
    that the dictionary values are all strings.
    """
    values = []

    for key in keys:
        value = d.get(key)
        if value is not None and type(value) is str:
            values.append(value)

    return " ".join(values)
