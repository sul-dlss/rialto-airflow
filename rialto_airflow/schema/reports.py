from sqlalchemy import Boolean, Column, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base  # type: ignore
from sqlalchemy.types import DateTime

from rialto_airflow.database import utcnow


# a database with a consistent name, to which we publish summary and denormalized data
# derived from harvests, for use by e.g. Tableau reports and visualizations
RIALTO_REPORTS_DB_NAME: str = (
    "rialto_reports"  # If you update this, update alembic_dbs in config/deploy.rb
)


ReportsSchemaBase = declarative_base()


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


class PublicationsBySchool(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications_by_school"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String)
    pub_year = Column(Integer)
    apc = Column(Integer)
    open_access = Column(String)
    types = Column(String)
    federally_funded = Column(Boolean)
    academic_council_authored = Column(Boolean)
    faculty_authored = Column(Boolean)
    primary_school = Column(String)


class PublicationsByDepartment(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications_by_department"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String)
    pub_year = Column(Integer)
    apc = Column(Integer)
    open_access = Column(String)
    types = Column(String)
    federally_funded = Column(Boolean)
    academic_council_authored = Column(Boolean)
    faculty_authored = Column(Boolean)
    primary_school = Column(String)
    primary_department = Column(String)


<<<<<<< HEAD
class PublicationsByAuthor(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications_by_author"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, nullable=True)
    sunet = Column(String, nullable=False)
    orcid = Column(String)
    abstract = Column(Text)
    academic_council = Column(String)
    apc = Column(String)
    author_list_names = Column(Text)
    author_list_orcids = Column(Text)
    citation_count = Column(Integer)
    federally_funded = Column(Boolean)
    first_author_name = Column(String)
    first_author_orcid = Column(String)
    funder_list_grid = Column(Text)
    funder_list_name = Column(Text)
    grant_ids = Column(Text)
    issue = Column(String)
    journal_issn = Column(String)
    journal_name = Column(String)
    last_author_name = Column(String)
    last_author_orcid = Column(String)
    open_access = Column(String)
    pages = Column(String)
    primary_department = Column(String)
    primary_school = Column(String)
    pub_year = Column(Integer)
    publisher = Column(String)
    role = Column(String)
    title = Column(Text)
    types = Column(String)
    volume = Column(String)
    __table_args__ = (
        UniqueConstraint("doi", "sunet", name="uq_publications_by_author_doi_sunet"),
    )


||||||| parent of 05dc643 (Add publications_by_author table)
=======
class PublicationsByAuthor(ReportsSchemaBase):  # type: ignore
    __tablename__ = "publications_by_author"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, nullable=True)
    sunet = Column(String, nullable=False)
    orcid = Column(String)
    abstract = Column(Text)
    academic_council = Column(String)
    apc = Column(String)
    author_list_names = Column(Text)
    author_list_orcids = Column(Text)
    citation_count = Column(Integer)
    federally_funded = Column(Boolean)
    first_author_name = Column(String)
    first_author_orcid = Column(String)
    funder_list_grid = Column(Text)
    funder_list_name = Column(Text)
    grant_ids = Column(Text)
    issue = Column(String)
    journal_issn = Column(String)
    journal_name = Column(String)
    last_author_name = Column(String)
    last_author_orcid = Column(String)
    open_access = Column(String)
    pages = Column(String)
    primary_department = Column(String)
    primary_school = Column(String)
    pub_year = Column(Integer)
    publisher = Column(String)
    role = Column(String)
    title = Column(Text)
    types = Column(String)
    volume = Column(String)
    __table_args__ = (
        UniqueConstraint(
            "doi", "sunet", name="uq_publications_by_author_doi_sunet"
        ),
    )


>>>>>>> 05dc643 (Add publications_by_author table)
class AuthorOrcids(ReportsSchemaBase):  # type: ignore
    __tablename__ = "author_orcids"

    sunetid = Column(String, primary_key=True)
    orcidid = Column(String)
    first_name = Column(String)
    last_name = Column(String)
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
    __tablename__ = "orcid_integration_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_label = Column(String)
    read_only_scope = Column(Integer)
    read_write_scope = Column(Integer)
