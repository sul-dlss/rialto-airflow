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


def get_csv_path(snapshot, google_drive_folder, filename) -> Path:
    """
    Get the base path for a CSV file in the shared google drive
    """
    csv_path = snapshot.path / google_drive_folder / filename
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    return csv_path


def piped(lst: list[str]) -> Optional[str]:
    """
    Return a list as pipe delimited or None if None is passed in.
    """
    if lst is None:
        return None
    return "|".join(lst)
