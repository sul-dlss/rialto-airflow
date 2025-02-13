import logging
import os
from pathlib import Path

from sqlalchemy import Table, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import expression
from sqlalchemy.types import DateTime


Base = declarative_base()


def engine_setup(database_name: str):
    """
    When creating the database and its schema, use an engine and connection.
    Subsequent querying should be done through a session.
    """
    engine = create_engine(
        f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/{database_name}", echo=True
    )
    return engine


def create_database(snapshot_dir: str) -> str:
    """Create a DAG-specific database for publications and author/orgs data"""
    timestamp = Path(snapshot_dir).name
    database_name = f"rialto_{timestamp}"

    # set up the connection using the default postgres database
    # see discussion here: https://stackoverflow.com/questions/6506578/how-to-create-a-new-database-using-sqlalchemy
    # and https://docs.sqlalchemy.org/en/14/core/connections.html#understanding-the-dbapi-level-autocommit-isolation-level
    postgres_conn = f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/postgres"
    engine = create_engine(postgres_conn)
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text(f"create database {database_name}"))
        connection.close()

    logging.info(f"created database {database_name}")
    return database_name


class utcnow(expression.FunctionElement):
    """
    Create a UTC timestamp
    https://docs.sqlalchemy.org/en/14/core/compiler.html#utc-timestamp-function
    """

    type = DateTime()
    inherit_cache = True


@compiles(utcnow, "postgresql")
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


pub_author_association = Table(
    "pub_author_association",
    Base.metadata,
    Column("publication_id", ForeignKey("publication.id"), primary_key=True),
    Column("author_id", ForeignKey("author.id"), primary_key=True),
)


class Publication(Base):
    __tablename__ = "publication"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, unique=True)
    title = Column(String)
    pub_year = Column(Integer)
    dim_json = Column(JSONB)
    openalex_json = Column(JSONB)
    sulpub_json = Column(JSONB)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())
    authors = relationship(
        "Author", secondary=pub_author_association, back_populates="publications"
    )


class Author(Base):
    __tablename__ = "author"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sunet = Column(String, unique=True)
    orcid = Column(String, unique=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    status = Column(String, nullable=False)
    academic_council = Column(Boolean)
    primary_role = Column(String)
    schools = Column(ARRAY(String))
    departments = Column(ARRAY(String))
    primary_school = Column(String)
    primary_dept = Column(String)
    primary_division = Column(String)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())
    publications = relationship(
        "Publication", secondary=pub_author_association, back_populates="authors"
    )


def create_schema(database_name: str):
    """Create tables for the publications and author/orgs data"""
    engine = engine_setup(database_name)
    with engine.connect() as connection:
        Base.metadata.create_all(engine)
        connection.close()

    logging.info(f"Created schema in database {database_name}")
