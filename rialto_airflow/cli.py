import csv
import sys

import dotenv
import typer
from typing_extensions import Annotated
from sqlalchemy import select

from rialto_airflow.database import get_session
from rialto_airflow.schema.rialto import RIALTO_DB_NAME
from rialto_airflow.schema.rialto import Author


dotenv.load_dotenv()
app = typer.Typer()


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
def authors(db_name: Annotated[str, typer.Option()] = "") -> None:
    """
    List the SUNET IDs for authors in the database.
    """
    with get_session(RIALTO_DB_NAME).begin() as session:
        for author in session.query(Author).all():
            print(author.sunet)


if __name__ == "__main__":
    app()
