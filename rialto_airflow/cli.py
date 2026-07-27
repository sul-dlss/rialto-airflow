import csv
import json
import os
import sys
from typing import Annotated

import dotenv
import typer
from sqlalchemy import select

from rialto_airflow.database import get_session
from rialto_airflow.schema.rialto import RIALTO_DB_NAME, Author, Publication

dotenv.load_dotenv()
app = typer.Typer()

# The names of the harvest providers, each of which is stored in a
# <provider>_json column on the Publication table.
PROVIDERS = ["sulpub", "crossref", "dim", "wos", "openalex", "pubmed"]


@app.callback()
def main(
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            "-d",
            help=(
                "PostgreSQL connection URL to use, without the database name, e.g. "
                "postgresql+psycopg2://user:password@host:5432 . This overrides the "
                "AIRFLOW_VAR_RIALTO_POSTGRES environment variable and makes it "
                "possible to run these commands against a remote database."
            ),
        ),
    ] = None,
) -> None:
    """
    Command line utilities for working with the RIALTO database.
    """
    if database_url is not None:
        os.environ["AIRFLOW_VAR_RIALTO_POSTGRES"] = database_url


@app.command()
def publications(sunet: str) -> None:
    """
    List publications for an Author with a given SUNET.
    """
    with get_session(RIALTO_DB_NAME).begin() as session:
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
                "sources",
            ],
        )

        writer.writeheader()

        for pub in author.publications:
            sources = []
            for source_name in [
                "sulpub",
                "crossref",
                "dim",
                "wos",
                "openalex",
                "pubmed",
            ]:
                if getattr(pub, f"{source_name}_json") is not None:
                    sources.append(source_name)

            writer.writerow(
                {
                    "doi": pub.doi,
                    "title": pub.title,
                    "publisher": pub.publisher,
                    "pub_year": pub.pub_year,
                    "open_access": pub.open_access,
                    "types": "|".join(pub.types),
                    "journal_name": pub.journal_name,
                    "authors": "|".join([a.sunet for a in pub.authors]),
                    "funders": "|".join([f.name for f in pub.funders]),
                    "sources": "|".join(sources),
                }
            )


@app.command()
def export(
    provider: str,
    output: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="File to write the JSON-L to. Defaults to stdout.",
        ),
    ] = "-",
) -> None:
    """
    Export the harvested JSON for a given provider as newline-delimited JSON
    (JSON-L), one publication per line. PROVIDER must be one of: sulpub,
    crossref, dim, wos, openalex, pubmed.
    """
    if provider not in PROVIDERS:
        print(
            f"Unknown provider {provider!r}. Choose from: {', '.join(PROVIDERS)}",
            file=sys.stderr,
        )
        raise typer.Exit(code=1)

    column = getattr(Publication, f"{provider}_json")

    out = sys.stdout if output == "-" else open(output, "w")
    try:
        with get_session(RIALTO_DB_NAME).begin() as session:
            # Stream just the JSON column. Setting yield_per as an execution
            # option (before execute) makes SQLAlchemy use a server-side cursor,
            # so rows are fetched in batches as we iterate rather than the whole
            # result set being buffered into client memory up front.
            stmt = (
                select(column)
                .where(column.is_not(None))
                .execution_options(yield_per=1000)
            )
            for (data,) in session.execute(stmt):
                out.write(json.dumps(data))
                out.write("\n")
    finally:
        if out is not sys.stdout:
            out.close()


@app.command()
def authors() -> None:
    """
    List the SUNET IDs for authors in the database.
    """
    with get_session(RIALTO_DB_NAME).begin() as session:
        for author in session.query(Author).all():
            print(author.sunet)


if __name__ == "__main__":
    app()
