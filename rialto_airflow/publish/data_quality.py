import csv
import json
import logging
from itertools import combinations
from collections import defaultdict
from pathlib import Path

import pandas
from sqlalchemy import select, and_, func
from sqlalchemy.engine.row import Row  # type: ignore

from rialto_airflow.database import Author, Publication, get_session
from rialto_airflow.distiller import JsonPathRule, first
from rialto_airflow.harvest.sul_pub import extract_doi
from rialto_airflow.utils import get_types, get_csv_path
from rialto_airflow.snapshot import Snapshot


def google_drive_folder() -> str:
    return "data-quality-dashboard"


def write_authors(snapshot: Snapshot) -> Path:
    """
    Read in existing authors.csv and write it out with additional columns useful
    for data quality reporting.
    """
    logging.info("started writing authors.csv")

    authors = pandas.read_csv(snapshot.path / "authors.csv")

    pub_count: defaultdict[str, int] = defaultdict(int)
    new_count: defaultdict[str, int] = defaultdict(int)
    approved_count: defaultdict[str, int] = defaultdict(int)
    denied_count: defaultdict[str, int] = defaultdict(int)
    unknown_count: defaultdict[str, int] = defaultdict(int)

    for line in (snapshot.path / "sulpub.jsonl").open():
        pub = json.loads(line)
        for author in pub["authorship"]:
            cap_id = author["cap_profile_id"]

            pub_count[cap_id] += 1

            match author["status"]:
                case "new":
                    new_count[cap_id] += 1
                case "approved":
                    approved_count[cap_id] += 1
                case "denied":
                    denied_count[cap_id] += 1
                case "unknown":
                    unknown_count[cap_id] += 1

    authors["pub_count"] = authors.apply(
        lambda a: pub_count.get(a.cap_profile_id, 0), axis=1
    )
    authors["new_count"] = authors.apply(
        lambda a: new_count.get(a.cap_profile_id, 0), axis=1
    )
    authors["approved_count"] = authors.apply(
        lambda a: approved_count.get(a.cap_profile_id, 0), axis=1
    )
    authors["denied_count"] = authors.apply(
        lambda a: denied_count.get(a.cap_profile_id, 0), axis=1
    )
    authors["unknown_count"] = authors.apply(
        lambda a: unknown_count.get(a.cap_profile_id, 0), axis=1
    )

    csv_path = get_csv_path(snapshot, google_drive_folder(), "authors.csv")

    authors.to_csv(csv_path, index=False)
    logging.info("finished writing authors.csv")

    return csv_path


def write_sulpub(snapshot: Snapshot) -> Path:
    col_names = ["doi", "year", "cap_profile_id", "status", "visibility"]

    logging.info("started writing sulpub.csv")

    csv_path = get_csv_path(snapshot, google_drive_folder(), "sulpub.csv")

    with csv_path.open("w") as output:
        csv_output = csv.DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        for line in (snapshot.path / "sulpub.jsonl").open("r"):
            pub = json.loads(line)

            csv_output.writerow(
                {
                    "doi": extract_doi(pub),
                    "year": pub.get("year"),
                    "cap_profile_id": _extract_authorship(pub, "cap_profile_id"),
                    "status": _extract_authorship(pub, "status"),
                    "visibility": _extract_authorship(pub, "visibility"),
                }
            )

    logging.info("finished writing sulpub.csv")

    return csv_path


def write_total_source_count(snapshot: Snapshot) -> Path:
    col_names = ["source", "total_count"]

    logging.info("started writing total-source-counts.csv")

    csv_path = get_csv_path(snapshot, google_drive_folder(), "total-source-counts.csv")

    with csv_path.open("w") as output:
        csv_output = csv.DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        # these are issued as SQL count queries for each source where DOI and JSON is not null
        # even though we issue a new query per source, each of them should be very efficient
        # since no data needs to be loaded into memory and iterated over
        with get_session(snapshot.database_name).begin() as session:
            dimensions_count = (
                session.query(func.count(Publication.id))
                .filter(
                    and_(
                        Publication.doi.is_not(None),  # type: ignore
                        Publication.dim_json.is_not(None),  # type: ignore
                    )
                )
                .scalar()
            )
            openalex_count = (
                session.query(func.count(Publication.id))
                .filter(
                    and_(
                        Publication.doi.is_not(None),  # type: ignore
                        Publication.openalex_json.is_not(None),  # type: ignore
                    )
                )
                .scalar()
            )
            pubmed_count = (
                session.query(func.count(Publication.id))
                .filter(
                    and_(
                        Publication.doi.is_not(None),  # type: ignore
                        Publication.pubmed_json.is_not(None),  # type: ignore
                    )
                )
                .scalar()
            )
            wos_count = (
                session.query(func.count(Publication.id))
                .filter(
                    and_(
                        Publication.doi.is_not(None),  # type: ignore
                        Publication.wos_json.is_not(None),  # type: ignore
                    )
                )
                .scalar()
            )

        csv_output.writerow({"source": "Dimensions", "total_count": dimensions_count})
        csv_output.writerow({"source": "Openalex", "total_count": openalex_count})
        csv_output.writerow({"source": "PubMed", "total_count": pubmed_count})
        csv_output.writerow({"source": "WoS", "total_count": wos_count})

    logging.info("finished writing total-source-counts.csv")

    return csv_path


def write_contributions_by_source(snapshot: Snapshot):
    col_names = ["doi", "source", "present", "pub_year", "open_access", "types"]

    logging.info("started writing contributions-by-source.csv")

    csv_path = get_csv_path(
        snapshot, google_drive_folder(), "contributions-by-source.csv"
    )

    with csv_path.open("w") as output:
        csv_output = csv.DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Author.sunet,  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.pub_year,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Publication.dim_json,
                    Publication.openalex_json,
                    Publication.sulpub_json,  # type: ignore
                    Publication.wos_json,  # type:ignore
                    Publication.dim_json["type"].label("dim_type"),
                    Publication.openalex_json["type"].label("openalex_type"),
                    Publication.wos_json["static_data"]["fullrecord_metadata"][
                        "normalized_doctypes"
                    ]["doctype"].label("wos_type"),
                )
                .join(Author, Publication.authors)  # type: ignore
                .where(Publication.pub_year >= 2018)
                .group_by(Author.id, Publication.id)
                .execution_options(yield_per=100)
            )

        for row in session.execute(stmt):
            for source in ["dim_json", "openalex_json", "sulpub_json", "wos_json"]:
                csv_output.writerow(
                    {
                        "doi": row.doi,
                        "source": source.replace("_json", ""),
                        "present": row._mapping[source] is not None,
                        "pub_year": row.pub_year,
                        "open_access": row.open_access,
                        "types": "|".join(get_types(row)),
                    }
                )

    logging.info("finished writing contributions-by-source.csv")

    return csv_path


def write_publications(snapshot: Snapshot) -> Path:
    col_names = [
        "any_url",
        "any_apc",
        "doi",
        "oa_url",
        "open_access",
        "openalex_apc_list",
        "openalex_apc_paid",
        "pub_year",
        "types",
    ]

    logging.info("started writing publications.csv")

    csv_path = get_csv_path(snapshot, google_drive_folder(), "publications.csv")

    with csv_path.open("w") as output:
        csv_output = csv.DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        with get_session(snapshot.database_name).begin() as session:
            stmt = (
                select(  # type: ignore
                    Publication.doi,  # type: ignore
                    Publication.pub_year,  # type: ignore
                    Publication.open_access,  # type: ignore
                    Publication.dim_json,
                    Publication.openalex_json,
                    Publication.sulpub_json,
                    Publication.wos_json,  # type: ignore
                    Publication.dim_json["type"].label("dim_type"),
                    Publication.openalex_json["type"].label("openalex_type"),
                    Publication.wos_json["static_data"]["fullrecord_metadata"][
                        "normalized_doctypes"
                    ]["doctype"].label("wos_type"),
                )
                .where(Publication.pub_year >= 2018)
                .execution_options(yield_per=1000)
            )

        for row in session.execute(stmt):
            csv_output.writerow(
                {
                    "any_url": _any_url(row),
                    "any_apc": _any_apc(row),
                    "doi": row.doi,
                    "oa_url": _oa_url(row),
                    "open_access": row.open_access,
                    "openalex_apc_list": _openalex_apc_list(row),
                    "openalex_apc_paid": _openalex_apc_paid(row),
                    "pub_year": row.pub_year,
                    "types": "|".join(get_types(row)),
                }
            )

    logging.info("finished writing publications.csv")

    return csv_path


def write_source_counts(snapshot):
    logging.info("started writing source-counts.csv")

    csv_path = get_csv_path(snapshot, google_drive_folder(), "source-counts.csv")

    source_labels = {
        "dim_json": "Dimensions",
        "openalex_json": "Openalex",
        "pubmed_json": "PubMed",
        "wos_json": "WoS",
    }

    sources = sorted(source_labels.keys())

    counts: dict[str, int] = {}

    with get_session(snapshot.database_name).begin() as session:
        stmt = (
            select(  # type: ignore
                Publication.id,  # type: ignore
                Publication.dim_json,
                Publication.openalex_json,  # type: ignore
                Publication.pubmed_json,
                Publication.wos_json,  # type: ignore
            )
            .where(Publication.doi is not None)
            .execution_options(yield_per=10000)
        )

        for row in session.execute(stmt):
            keys = []
            for source in sources:
                if row._mapping[source] is not None:
                    keys.append(source_labels[source])

            key = "|".join(keys)

            if key in counts:
                counts[key] += 1
            else:
                counts[key] = 1

    with csv_path.open("w") as output:
        csv_output = csv.writer(output)
        csv_output.writerow(["sources", "count"])
        for combo in _combos(list(source_labels.values())):
            csv_output.writerow([combo, counts.get(combo, 0)])

    return csv_path


def _any_url(row: Row):
    return first(
        row,
        rules=[
            JsonPathRule("openalex_json", "best_oa_location.pdf_url"),
            JsonPathRule("openalex_json", "open_access.oa_url"),
            JsonPathRule("openalex_json", "primary_location.pdf_url"),
        ],
    )


def _any_apc(row: Row):
    return first(
        row,
        rules=[
            JsonPathRule("openalex_json", "apc_paid.value_usd"),
            JsonPathRule("openalex_json", "apc_list.value_usd"),
        ],
    )


def _oa_url(row: Row):
    return first(
        row,
        rules=[
            JsonPathRule("openalex_json", "best_oa_location.pdf_url"),
            JsonPathRule("openalex_json", "open_access.oa_url"),
            JsonPathRule("openalex_json", "primary_location.pdf_url"),
        ],
    )


def _openalex_apc_list(row: Row):
    return first(
        row,
        rules=[
            JsonPathRule("openalex_json", "apc_list.value_usd"),
        ],
    )


def _openalex_apc_paid(row: Row):
    return first(
        row,
        rules=[
            JsonPathRule("openalex_json", "apc_paid.value_usd"),
        ],
    )


def _extract_authorship(pub: dict, name: str) -> str:
    """
    Some status and visibility values are set to null in the JSON. Also some are
    in all caps. This ensures that the string "none" is returned instead of
    None, and also that the values are lowercased.
    """
    values = []

    for author in pub["authorship"]:
        if author.get(name) is not None:
            values.append(str(author.get(name)).lower())
        else:
            values.append("none")

    return "|".join(values)


def _combos(values: list[str]) -> list[str]:
    """
    Generate a list of all possible combinations of the strings in a given list.
    Each combination is represented as a pipe delimited string.

    >>> _combos(['a', 'b', 'c'])
    [ 'a', 'b', 'c', 'a|b', 'a|b|c', 'a|c', 'b|c']

    """
    combos = []

    # The itertools.combinations function will generate possible combinations
    # for a given list and desired number of elements in each combination.
    # So if we have a list of 3 items, we can ask for all the possible
    # combinations with 1 element, all the possible combinations with 2
    # elements, all the possible combinations with 3 elements, and then combine
    # them together.

    for combo_group in [
        list(combinations(values, n)) for n in range(1, len(values) + 1)
    ]:
        for combo in combo_group:
            combos.append("|".join(combo))

    return sorted(combos)
