from pathlib import Path
import re


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

    doi = doi.strip().lower()
    doi = doi.replace("https://doi.org/", "").replace("https://dx.doi.org/", "")
    doi = re.sub("^doi: ", "", doi)

    return doi


def normalize_orcid(orcid):
    orcid = orcid.strip().lower()
    orcid = orcid.replace("https://orcid.org/", "").replace(
        "https://sandbox.orcid.org/", ""
    )

    return orcid
