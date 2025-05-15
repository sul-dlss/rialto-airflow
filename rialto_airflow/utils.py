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
    doi = re.sub(r"^doi:\s?", "", doi)

    return doi


def normalize_orcid(orcid):
    orcid = orcid.strip().lower()
    orcid = orcid.replace("https://orcid.org/", "").replace(
        "https://sandbox.orcid.org/", ""
    )

    return orcid


def get_types(row):
    types = set()
    if row.dim_type:
        types.add(row.dim_type)
    if row.openalex_type:
        types.add(row.openalex_type)
    # the wos type can be a single value or a list
    if row.wos_type:
        if isinstance(row.wos_type, list):
            for wos_type in row.wos_type:
                types.add(wos_type.lower())
        else:
            types.add(row.wos_type.lower())

    return sorted(types)


def get_csv_path(snapshot, google_drive_folder, filename) -> Path:
    """
    Get the base path for a CSV file in the shared google drive
    """
    csv_path = snapshot.path / google_drive_folder / filename
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    return csv_path
