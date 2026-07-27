from sqlalchemy import Boolean, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.types import DateTime

from rialto_airflow.database import utcnow

# a database with a consistent name, to which we publish summary and denormalized data
# derived from harvests, for use by e.g. Tableau reports and visualizations
RIALTO_REPORTS_DB_NAME: str = (
    "rialto_reports"  # If you update this, update alembic_dbs in config/deploy.rb
)


ReportsSchemaBase = declarative_base()


class Publications(ReportsSchemaBase):
    __tablename__ = "publications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doi: Mapped[str | None] = mapped_column(String, unique=True)
    pub_year: Mapped[int | None] = mapped_column(Integer)
    apc: Mapped[int | None] = mapped_column(Integer)
    author_list_names: Mapped[str | None] = mapped_column(Text)
    open_access: Mapped[str | None] = mapped_column(String)
    types: Mapped[str | None] = mapped_column(String)
    publisher: Mapped[str | None] = mapped_column(String)
    journal_name: Mapped[str | None] = mapped_column(String)
    title: Mapped[str | None] = mapped_column(Text)
    federally_funded: Mapped[bool | None] = mapped_column(Boolean)
    academic_council_authored: Mapped[bool | None] = mapped_column(Boolean)
    faculty_authored: Mapped[bool | None] = mapped_column(Boolean)


class PublicationsBySchool(ReportsSchemaBase):
    __tablename__ = "publications_by_school"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doi: Mapped[str | None] = mapped_column(String)
    pub_year: Mapped[int | None] = mapped_column(Integer)
    apc: Mapped[int | None] = mapped_column(Integer)
    open_access: Mapped[str | None] = mapped_column(String)
    types: Mapped[str | None] = mapped_column(String)
    federally_funded: Mapped[bool | None] = mapped_column(Boolean)
    academic_council_authored: Mapped[bool | None] = mapped_column(Boolean)
    faculty_authored: Mapped[bool | None] = mapped_column(Boolean)
    primary_school: Mapped[str | None] = mapped_column(String)


class PublicationsByDepartment(ReportsSchemaBase):
    __tablename__ = "publications_by_department"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doi: Mapped[str | None] = mapped_column(String)
    pub_year: Mapped[int | None] = mapped_column(Integer)
    apc: Mapped[int | None] = mapped_column(Integer)
    open_access: Mapped[str | None] = mapped_column(String)
    types: Mapped[str | None] = mapped_column(String)
    federally_funded: Mapped[bool | None] = mapped_column(Boolean)
    academic_council_authored: Mapped[bool | None] = mapped_column(Boolean)
    faculty_authored: Mapped[bool | None] = mapped_column(Boolean)
    primary_school: Mapped[str | None] = mapped_column(String)
    primary_department: Mapped[str | None] = mapped_column(String)


class PublicationsByAuthor(ReportsSchemaBase):
    __tablename__ = "publications_by_author"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doi: Mapped[str | None] = mapped_column(String)
    sunet: Mapped[str] = mapped_column(String, nullable=False)
    orcid: Mapped[str | None] = mapped_column(String)
    abstract: Mapped[str | None] = mapped_column(Text)
    academic_council: Mapped[bool | None] = mapped_column(Boolean)
    apc: Mapped[int | None] = mapped_column(Integer)
    author_list_names: Mapped[str | None] = mapped_column(Text)
    author_list_orcids: Mapped[str | None] = mapped_column(Text)
    citation_count: Mapped[int | None] = mapped_column(Integer)
    federally_funded: Mapped[bool | None] = mapped_column(Boolean)
    first_author_name: Mapped[str | None] = mapped_column(String)
    first_author_orcid: Mapped[str | None] = mapped_column(String)
    grant_ids: Mapped[str | None] = mapped_column(Text)
    issue: Mapped[str | None] = mapped_column(String)
    journal_name: Mapped[str | None] = mapped_column(String)
    last_author_name: Mapped[str | None] = mapped_column(String)
    last_author_orcid: Mapped[str | None] = mapped_column(String)
    open_access: Mapped[str | None] = mapped_column(String)
    pages: Mapped[str | None] = mapped_column(String)
    primary_department: Mapped[str | None] = mapped_column(String)
    primary_school: Mapped[str | None] = mapped_column(String)
    pub_year: Mapped[int | None] = mapped_column(Integer)
    publisher: Mapped[str | None] = mapped_column(String)
    role: Mapped[str | None] = mapped_column(String)
    title: Mapped[str | None] = mapped_column(Text)
    types: Mapped[str | None] = mapped_column(String)
    volume: Mapped[str | None] = mapped_column(String)
    __table_args__ = (
        UniqueConstraint("doi", "sunet", name="uq_publications_by_author_doi_sunet"),
    )


class AuthorOrcids(ReportsSchemaBase):
    __tablename__ = "author_orcids"

    sunetid: Mapped[str] = mapped_column(String, primary_key=True)
    orcidid: Mapped[str | None] = mapped_column(String)
    first_name: Mapped[str | None] = mapped_column(String)
    last_name: Mapped[str | None] = mapped_column(String)
    orcid_update_scope: Mapped[bool | None] = mapped_column(Boolean)
    role: Mapped[str | None] = mapped_column(String)
    primary_affiliation: Mapped[str | None] = mapped_column(String)
    primary_school: Mapped[str | None] = mapped_column(String)
    primary_department: Mapped[str | None] = mapped_column(String)
    primary_division: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[DateTime | None] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, onupdate=utcnow())


class OrcidIntegrationStats(ReportsSchemaBase):
    __tablename__ = "orcid_integration_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date_label: Mapped[str | None] = mapped_column(String)
    read_only_scope: Mapped[int | None] = mapped_column(Integer)
    read_write_scope: Mapped[int | None] = mapped_column(Integer)
