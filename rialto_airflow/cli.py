import os
import csv
import sys
from typing import Optional

import dotenv
import typer
from typing_extensions import Annotated
from sqlalchemy import select
from sqlalchemy.orm.session import sessionmaker

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import Author


dotenv.load_dotenv()
app = typer.Typer()


@app.command()
def snapshots(
    db_uri: Annotated[
        str,
        typer.Option(
            help="Override the default Postgres database URI from the environment"
        ),
    ] = "",
) -> None:
    """
    List Rialto snapshot databases that are available.
    """
    for database in _get_snapshots(db_uri):
        print(database)


@app.command()
def publications(
    sunet: Annotated[
        str,
        typer.Argument(
            help="The SUNET of the author you want to list publications for"
        ),
    ],
    db_name: str = typer.Option(
        default="", help="The snapshot database to use (use snapshots to list them)"
    ),
    db_uri: str = typer.Option(
        default="",
        help="Override the default Postgres database URI from the environment",
    ),
) -> None:
    """
    List publications for an Author with a given SUNET.
    """
    with _get_db(db_uri, db_name).begin() as session:  # type: ignore
        author = (
            session.execute(select(Author).where(Author.sunet == sunet))
            .scalars()
            .first()
        )

        if author is None:
            print(f"The author {sunet} does not exist")
            raise typer.Exit(code=1)

        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=[
                "doi",
                "title",
                "publisher",
                "pub_year",
                "open_access",
                "types",
                "journal_name",
                "authors",
                "funders",
                "sulpub_id",
                "crossref_id",
                "dimensions_id",
                "wos_id",
                "openalex_id",
                "pubmed_id",
            ],
        )

        writer.writeheader()

        for pub in author.publications:
            writer.writerow(
                {
                    "doi": pub.doi,
                    "title": pub.title,
                    "publisher": pub.publisher,
                    "pub_year": pub.pub_year,
                    "open_access": pub.open_access,
                    "types": "|".join(pub.types or []),
                    "journal_name": pub.journal_name,
                    "authors": "|".join([a.sunet for a in pub.authors]),
                    "funders": "|".join([f.name for f in pub.funders]),
                    "sulpub_id": _sulpub_id(pub),
                    "crossref_id": _crossref_id(pub),
                    "dimensions_id": _dimensions_id(pub),
                    "wos_id": _wos_id(pub),
                    "openalex_id": _openalex_id(pub),
                    "pubmed_id": _pubmed_id(pub),
                }
            )


@app.command()
def authors(
    db_name: str = typer.Option(default="", help="The Rialto database snapshot to use"),
    db_uri: str = typer.Option(
        default="", help="Override the Postgres database URI from the environment"
    ),
) -> None:
    """
    List the SUNET IDs for authors in the snapshot.
    """
    with _get_db(db_uri, db_name).begin() as session:  # type: ignore
        for author in session.query(Author).all():
            print(author.sunet)


def _get_db(db_uri: str, db_name: str) -> sessionmaker:
    """
    Given a database URI and database name, return the a sqlsession object.
    """

    # optionally override the default database URI if it was supplied
    # this can be useful if you want to connect to a PostgreSQL database running
    # somewhere else
    if db_uri != "":
        os.environ["AIRFLOW_VAR_RIALTO_POSTGRES"] = db_uri

    if db_name == "":
        db_name = list(_get_snapshots(db_uri)).pop()

    return get_session(db_name)


def _get_snapshots(db_uri: str):
    with _get_db(db_uri, "postgres").begin() as session:  # type: ignore
        for row in session.execute(
            r"SELECT datname FROM pg_database WHERE datname ~ '^rialto_\d+' ORDER BY datname ASC"
        ):
            yield row[0]


def _sulpub_id(pub) -> Optional[str]:
    if pub.sulpub_json is not None:
        return pub.sulpub_json["sulpubid"]
    else:
        return None


def _crossref_id(pub) -> Optional[str]:
    if pub.crossref_json is not None:
        return pub.crossref_json.get("DOI")
    else:
        return None


def _dimensions_id(pub) -> Optional[str]:
    if pub.dim_json is not None:
        return pub.dim_json.get("id")
    else:
        return None


def _wos_id(pub) -> Optional[str]:
    if pub.wos_json is not None:
        return pub.wos_json.get("UID")
    return None


def _openalex_id(pub) -> Optional[str]:
    if pub.openalex_json is not None:
        return pub.openalex_json.get("id")
    else:
        return None


def _pubmed_id(pub) -> Optional[str]:
    if pub.pubmed_json is not None:
        return pub.pubmed_json.get("MedlineCitation", {}).get("PMID", {}).get("#text")
    else:
        return None


if __name__ == "__main__":
    app()
