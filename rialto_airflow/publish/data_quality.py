import json
import logging
from collections import defaultdict
from csv import DictWriter
from pathlib import Path

import pandas
from sqlalchemy import select
from sqlalchemy.engine.row import Row  # type: ignore

from rialto_airflow.database import Author, Publication, get_session
from rialto_airflow.distiller import JsonPathRule, first
from rialto_airflow.harvest.sul_pub import extract_doi
from rialto_airflow.publish.openaccess import get_types
from rialto_airflow.snapshot import Snapshot


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

    csv_path = snapshot.path / "data-quality-dashboard" / "authors.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    authors.to_csv(csv_path, index=False)
    logging.info("finished writing authors.csv")

    return csv_path


def write_sulpub(snapshot: Snapshot) -> Path:
    col_names = ["doi", "year", "cap_profile_id", "status", "visibility"]

    logging.info("started writing sulpub.csv")

    csv_path = snapshot.path / "data-quality-dashboard" / "sulpub.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
        csv_output.writeheader()

        for line in (snapshot.path / "sulpub.jsonl").open("r"):
            pub = json.loads(line)

            csv_output.writerow(
                {
                    "doi": extract_doi(pub),
                    "year": pub.get("year"),
                    "cap_profile_id": "|".join(
                        [str(a["cap_profile_id"]) for a in pub["authorship"]]
                    ),
                    "status": "|".join([a["status"] for a in pub["authorship"]]),
                    "visibility": "|".join(
                        [a["visibility"] for a in pub["authorship"]]
                    ),
                }
            )

    logging.info("finished writing sulpub.csv")

    return csv_path


def write_contributions_by_source(snapshot: Snapshot):
    col_names = ["doi", "source", "present", "pub_year", "open_access", "types"]

    logging.info("started writing contributions-by-source.csv")

    csv_path = snapshot.path / "data-quality-dashboard" / "contributions-by-source.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
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
                        "present": row[source] is not None,
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

    csv_path = snapshot.path / "data-quality-dashboard" / "publications.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w") as output:
        csv_output = DictWriter(output, fieldnames=col_names)
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
                .execution_options(yield_per=100)
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
