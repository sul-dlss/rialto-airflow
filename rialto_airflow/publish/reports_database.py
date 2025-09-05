from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base  # type: ignore
from sqlalchemy.types import DateTime

from rialto_airflow.database import (
    RIALTO_REPORTS_DB_NAME,
    create_schema,
    utcnow,
)

ReportsSchemaBase = declarative_base()

# NOTE: We used to write out CSV files to google drive as well.
# This was removed in https://github.com/sul-dlss/rialto-airflow/pull/528 in case
# we need it again in the future.  This PR also removed the related CSV writing tests in test_publication.py


class Publications(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, unique=True)
    pub_year = Column(Integer)
    apc = Column(Integer)
    open_access = Column(String)
    types = Column(String)
    federally_funded = Column(Boolean)
    academic_council_authored = Column(Boolean)
    faculty_authored = Column(Boolean)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())


class AuthorOrcids(ReportsSchemaBase):  # type: ignore
    __tablename__ = "author_orcids"

    orcidid = Column(String, primary_key=True)
    sunetid = Column(String)
    full_name = Column(String)
    orcid_update_scope = Column(Boolean)
    role = Column(String)
    primary_affiliation = Column(String)
    primary_school = Column(String)
    primary_department = Column(String)
    primary_division = Column(String)
    created_at = Column(DateTime, server_default=utcnow())
    updated_at = Column(DateTime, onupdate=utcnow())


class OrcidIntegrationStats(ReportsSchemaBase):  # type: ignore
    __tablename__ = "orcid_integrations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_label = Column(String)
    read_only_scope = Column(Integer)
    read_write_scope = Column(Integer)


def init_reports_data_schema() -> None:
    create_schema(RIALTO_REPORTS_DB_NAME, ReportsSchemaBase)
