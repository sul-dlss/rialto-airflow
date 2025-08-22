import logging
import os
from functools import cache

from sqlalchemy import Table, Boolean, Column, ForeignKey, Integer, String, Index
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import (  # type: ignore
    RelationshipProperty,
    declarative_base,
    relationship,
    sessionmaker,
)
from sqlalchemy.sql import expression
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.types import DateTime


Base = declarative_base()


def db_uri(database_name):
    return f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/{database_name}"


def engine_setup(database_name: str, pool_pre_ping=False, echo=False):
    """
    When creating the database and its schema, use an engine and connection.
    Subsequent querying should be done by accessing an engine via get_engine.
    """
    return create_engine(db_uri(database_name), pool_pre_ping=pool_pre_ping, echo=echo)


@cache
def get_engine(database_name: str, echo=False):
    """Memoized engine for use in other modules"""
    return engine_setup(database_name, pool_pre_ping=True, echo=echo)


def get_session(database_name: str):
    return sessionmaker(engine_setup(database_name))


def create_database(database_name: str) -> str:
    """Create a DAG-specific database for publications and author/orgs data"""

    # set up the connection using the default postgres database
    # see discussion here: https://stackoverflow.com/questions/6506578/how-to-create-a-new-database-using-sqlalchemy
    # and https://docs.sqlalchemy.org/en/14/core/connections.html#understanding-the-dbapi-level-autocommit-isolation-level
    engine = engine_setup("postgres")
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text(f"create database {database_name}"))
    logging.info(f"Created database {database_name}")
    return database_name


def drop_database(database_name: str):
    """Drop a DAG-specific database for publications and author/orgs data"""

    # set up the connection using the default postgres database
    engine = engine_setup("postgres")
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text(f"drop database {database_name}"))
    logging.info(f"Dropped database {database_name}")


def database_exists(database_name: str) -> bool:
    """Checks if a database with the given name exists"""

    engine = engine_setup("postgres")
    with engine.connect() as connection:
        result = connection.execute(
            text(
                "SELECT datname FROM pg_database WHERE datname = :db_name AND datistemplate = false"
            ),
            {"db_name": database_name},
        )
        # If the query returns any rows, the database exists
        return result.fetchone() is not None


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


pub_funder_association = Table(
    "pub_funder_association",
    Base.metadata,
    Column("publication_id", ForeignKey("publication.id"), primary_key=True),
    Column("funder_id", ForeignKey("funder.id"), primary_key=True),
)


class Publication(Base):  # type: ignore
    __tablename__ = "publication"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String)
    title = Column(String)
    pub_year = Column(Integer)
    open_access = Column(String)
    apc = Column(Integer)
    dim_json = Column(JSONB(none_as_null=True))
    openalex_json = Column(JSONB(none_as_null=True))
    sulpub_json = Column(JSONB(none_as_null=True))
    wos_json = Column(JSONB(none_as_null=True))
    pubmed_json = Column(JSONB(none_as_null=True))
    crossref_json = Column(JSONB(none_as_null=True))
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())
    authors: RelationshipProperty = relationship(
        "Author", secondary=pub_author_association, back_populates="publications"
    )
    funders: RelationshipProperty = relationship(
        "Funder", secondary=pub_funder_association, back_populates="publications"
    )

    __table_args__ = (
        Index(
            "doi_wos_idx",
            coalesce(doi, ""),
            coalesce(text("(wos_json ->> 'UID')"), ""),
            unique=True,
        ),
        Index(
            "doi_dim_idx",
            coalesce(doi, ""),
            coalesce(text("(dim_json ->> 'id')"), ""),
            unique=True,
        ),
    )


class Author(Base):  # type: ignore
    __tablename__ = "author"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sunet = Column(String, unique=True)
    cap_profile_id = Column(String, unique=True)
    orcid = Column(String, unique=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    status = Column(Boolean)
    academic_council = Column(Boolean)
    primary_role = Column(String)
    schools = Column(ARRAY(String))
    departments = Column(ARRAY(String))
    primary_school = Column(String)
    primary_dept = Column(String)
    primary_division = Column(String)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())
    publications: RelationshipProperty = relationship(
        "Publication", secondary=pub_author_association, back_populates="authors"
    )


class Funder(Base):  # type: ignore
    __tablename__ = "funder"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    grid_id = Column(String, unique=True)
    ror_id = Column(String, unique=True)
    openalex_id = Column(String, unique=True)
    federal = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())
    publications: RelationshipProperty = relationship(
        "Publication", secondary=pub_funder_association, back_populates="funders"
    )


def create_schema(database_name: str):
    """Create tables for the publications and author/orgs data"""
    engine = engine_setup(database_name)
    with engine.connect() as connection:
        Base.metadata.create_all(engine)
        connection.close()

    logging.info(f"Created schema in database {database_name}")


def get_index(klass, name) -> Index:
    """
    A helper function to look up an index on a model. Maybe there is a SQLAlchemy way of doing this?
    """
    return next(filter(lambda index: index.name == name, klass.__table__.indexes))
