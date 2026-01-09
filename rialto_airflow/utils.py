import re
import logging
from functools import cache
from pathlib import Path
from typing import Optional, Any


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


def downloads_dir(data_dir):
    """Get the path to the rialto data downloads directory"""
    data_path = Path(data_dir) / "downloads"

    if data_path.is_dir():
        return str(data_path)
    else:
        raise Exception(f"downloads directory missing at {data_path}")


@cache
def doi_candidate_regex():
    """
    One of the few things we can truly rely on when it comes to DOI format is that it will start with '10.'
    """
    return re.compile("10\\..+")


def doi_candidate_extract(possible_doi_with_junk: str):
    m = doi_candidate_regex().search(possible_doi_with_junk)
    return m.group(0) if m else None


@cache
def doi_known_format_regex_list():
    """
    filter patterns are adapted from suggestions from these sources (mostly from the Crossref blog post):
    * https://stackoverflow.com/a/10324802
    * https://www.crossref.org/blog/dois-and-matching-regular-expressions/ (via a different answer to the above question)
    """
    doi_filter_patterns = [
        "^10\\.\\d{4,9}/[-\\._;()/:a-zA-Z0-9]+$",  # matches e.g. '10.123456789/publication.ABC.123_123-a(23).1239'
        "^10\\.1002/[^\\s]+$",  # matches e.g. '10.1002/abc.1542.df'
        "^10\\.\\d{4}/\\d+-\\d+X?\\(\\d+\\)\\d+<[\\d\\w]+:[\\d\\w]*>\\d+\\.\\d+\\.\\w+;\\d$",  # matches e.g. '10.8888/7654-321X(12345)12345<1a2b3c:d4e5>1.2.a;1'
        "^10\\.1021/\\w\\w\\d+$",  # matches e.g. '10.1021/a_1029384756'
        "^10\\.1207/[\\w\\d]+\\&\\d+_\\d+$",  # matches e.g. '10.1207/a12b0z&456_765'
        "^\\b(10[\\.][0-9]{4,}(?:[\\.][0-9]+)*/(?:(?![\"&'<>])\\S)+)\\b$",  # matches e.g. '10.1016.12.31/nature.S0735-1097(98)2000/12/31/34:7-7' (SO post builds to this regex)
    ]
    return [re.compile(p) for p in doi_filter_patterns]


def matches_known_doi_format(possible_doi: str):
    return len([r for r in doi_known_format_regex_list() if r.match(possible_doi)]) > 0


@cache
def normalize_arxiv_id_to_doi(possible_arxiv_doi: str):
    """
    If we have the arXiv ID, we can turn it into a DOI.
    Per https://info.arxiv.org/help/doi.html
    "An author can determine their article’s DOI by using the DOI prefix
    https://doi.org/10.48550/ followed by the arXiv ID (replacing the
    colon with a period). For example, the arXiv ID arXiv:2202.01037 will
    translate to the DOI link https://doi.org/10.48550/arXiv.2202.01037."
    """
    return re.sub("^arxiv:", "10.48550/arxiv.", possible_arxiv_doi, flags=re.IGNORECASE)


def normalize_doi(doi):
    if doi is None or doi.strip() == "":
        return None

    context = {"doi_candidate": doi}

    doi = doi.lower().replace(" ", "")
    doi = doi.replace("\\", "")
    doi = normalize_arxiv_id_to_doi(doi)
    doi = doi_candidate_extract(doi)

    if doi is None:
        _data_quality_warning(
            "No DOI-like pattern in candidate string", context=context
        )
        return None

    if not matches_known_doi_format(doi):
        _data_quality_warning(
            "DOI-like string starts with 10. but doesn't fit more specific DOI pattern",
            context=context,
        )

    return doi


def _data_quality_warning(base_error_message: str, context: dict = {}):
    logging.warning(f"[DATA QUALITY ISSUE] {base_error_message} -- {context}")


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

    # ensure the list doesn't contain None
    lst = list(filter(lambda item: item is not None, lst))

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


def add_orcid(data: dict[Any, Any], orcid: str):
    """
    Envelope the provided dictionary inside another dictionary with the addition
    of an orcid key.
    """
    return {"orcid": orcid, "data": data}
