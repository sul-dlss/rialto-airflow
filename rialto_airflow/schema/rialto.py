import datetime
from typing import List, Optional

from sqlalchemy import (
    Table,
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import (
    Mapped,
    declarative_base,
    mapped_column,
    relationship,
)
from sqlalchemy.types import DateTime

from rialto_airflow.database import get_session
from rialto_airflow.database import utcnow

# permanent database for incrementally harvested data
RIALTO_DB_NAME: str = "rialto"

RialtoSchemaBase = declarative_base()

pub_author_association = Table(
    "pub_author_association",
    RialtoSchemaBase.metadata,
    Column(
        "publication_id",
        ForeignKey("publication.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("author_id", ForeignKey("author.id", ondelete="CASCADE"), primary_key=True),
)


pub_funder_association = Table(
    "pub_funder_association",
    RialtoSchemaBase.metadata,
    Column("publication_id", ForeignKey("publication.id"), primary_key=True),
    Column("funder_id", ForeignKey("funder.id"), primary_key=True),
)


class Publication(RialtoSchemaBase):
    __tablename__ = "publication"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doi: Mapped[Optional[str]] = mapped_column(String, unique=True)
    title: Mapped[Optional[str]] = mapped_column(String)
    pub_year: Mapped[Optional[int]] = mapped_column(Integer)
    open_access: Mapped[Optional[str]] = mapped_column(String)
    apc: Mapped[Optional[int]] = mapped_column(Integer)
    dim_json: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True))
    openalex_json: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True))
    sulpub_json: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True))
    wos_json: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True))
    pubmed_json: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True))
    crossref_json: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True))
    wos_id: Mapped[Optional[str]] = mapped_column(String)
    pubmed_id: Mapped[Optional[str]] = mapped_column(String)
    openalex_harvested: Mapped[Optional[DateTime]] = mapped_column(DateTime)
    dim_harvested: Mapped[Optional[DateTime]] = mapped_column(DateTime)
    sulpub_harvested: Mapped[Optional[DateTime]] = mapped_column(DateTime)
    wos_harvested: Mapped[Optional[DateTime]] = mapped_column(DateTime)
    pubmed_harvested: Mapped[Optional[DateTime]] = mapped_column(DateTime)
    created_at: Mapped[Optional[DateTime]] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[Optional[DateTime]] = mapped_column(DateTime, onupdate=utcnow())
    types: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    publisher: Mapped[Optional[str]] = mapped_column(String)
    journal_name: Mapped[Optional[str]] = mapped_column(String)
    academic_council_authored: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=False
    )
    faculty_authored: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    authors: Mapped[List["Author"]] = relationship(
        "Author",
        secondary=pub_author_association,
        back_populates="publications",
        cascade="all, delete",
    )
    funders: Mapped[List["Funder"]] = relationship(
        "Funder", secondary=pub_funder_association, back_populates="publications"
    )

    __table_args__ = (
        Index("idx_openalex_id", text("(openalex_json->>'id')")),
        Index("idx_wos_id", text("(wos_json->>'UID')")),
        Index("idx_sulpub_id", text("(sulpub_json->>'sulpubid')")),
        Index("idx_dim_id", text("(dim_json->>'id')")),
        Index("idx_pub_wos_id", "wos_id"),
        Index("idx_pub_pubmed_id", "pubmed_id"),
    )


class Author(RialtoSchemaBase):
    __tablename__ = "author"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sunet: Mapped[Optional[str]] = mapped_column(String, unique=True)
    cap_profile_id: Mapped[Optional[str]] = mapped_column(String, unique=True)
    orcid: Mapped[Optional[str]] = mapped_column(String, unique=True)
    first_name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[Optional[bool]] = mapped_column(Boolean)
    academic_council: Mapped[Optional[bool]] = mapped_column(Boolean)
    role: Mapped[Optional[str]] = mapped_column(String)
    schools: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    departments: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    primary_school: Mapped[Optional[str]] = mapped_column(String)
    primary_dept: Mapped[Optional[str]] = mapped_column(String)
    primary_division: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[Optional[DateTime]] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[Optional[DateTime]] = mapped_column(DateTime, onupdate=utcnow())
    publications: Mapped[List["Publication"]] = relationship(
        "Publication", secondary=pub_author_association, back_populates="authors"
    )


class Funder(RialtoSchemaBase):
    __tablename__ = "funder"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    grid_id: Mapped[Optional[str]] = mapped_column(String, unique=True)
    ror_id: Mapped[Optional[str]] = mapped_column(String, unique=True)
    openalex_id: Mapped[Optional[str]] = mapped_column(String, unique=True)
    federal: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    created_at: Mapped[Optional[DateTime]] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[Optional[DateTime]] = mapped_column(DateTime, onupdate=utcnow())
    publications: Mapped[List["Publication"]] = relationship(
        "Publication", secondary=pub_funder_association, back_populates="funders"
    )


class Harvest(RialtoSchemaBase):
    __tablename__ = "harvest"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finished_at: Mapped[Optional[DateTime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[Optional[DateTime]] = mapped_column(
        DateTime, server_default=utcnow()
    )

    @classmethod
    def create(cls) -> "Harvest":
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = cls()
            session.add(harvest)
            # Sends INSERT now (before commit), so autogenerated fields like id are available.
            session.flush()
            # Returned object is detached (safe to return outside session scope)
            session.expunge(harvest)
            return harvest

    @classmethod
    def get_by_id(cls, harvest_id: int) -> "Harvest | None":
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = session.get(cls, harvest_id)
            if harvest is not None:
                session.expunge(harvest)
            return harvest

    @classmethod
    def get_previous(cls) -> "Harvest | None":
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = session.execute(
                select(cls)
                .where(cls.finished_at.is_not(None))
                .order_by(cls.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if harvest is not None:
                session.expunge(harvest)
            return harvest

    def complete(self) -> None:
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = session.get(Harvest, self.id)
            if harvest is None:
                raise ValueError(f"Harvest {self.id} not found")
            harvest.finished_at = datetime.datetime.now(datetime.timezone.utc)
