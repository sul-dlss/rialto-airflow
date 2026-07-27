import datetime

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
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

from rialto_airflow.database import get_session, utcnow

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
    Column(
        "publication_id",
        ForeignKey("publication.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("funder_id", ForeignKey("funder.id"), primary_key=True),
)


class Publication(RialtoSchemaBase):
    __tablename__ = "publication"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doi: Mapped[str | None] = mapped_column(String, unique=True)
    title: Mapped[str | None] = mapped_column(String)
    pub_year: Mapped[int | None] = mapped_column(Integer)
    open_access: Mapped[str | None] = mapped_column(String)
    apc: Mapped[int | None] = mapped_column(Integer)
    dim_json: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    openalex_json: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    sulpub_json: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    wos_json: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    pubmed_json: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    crossref_json: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    wos_id: Mapped[str | None] = mapped_column(String)
    pubmed_id: Mapped[str | None] = mapped_column(String)
    openalex_harvested: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    dim_harvested: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    sulpub_harvested: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    wos_harvested: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    pubmed_harvested: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    distilled_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, default=utcnow(), onupdate=utcnow()
    )
    types: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    publisher: Mapped[str | None] = mapped_column(String)
    journal_name: Mapped[str | None] = mapped_column(String)
    academic_council_authored: Mapped[bool | None] = mapped_column(
        Boolean, default=False
    )
    faculty_authored: Mapped[bool | None] = mapped_column(Boolean, default=False)
    authors: Mapped[list["Author"]] = relationship(
        "Author",
        secondary=pub_author_association,
        back_populates="publications",
        cascade="all, delete",
    )
    funders: Mapped[list["Funder"]] = relationship(
        "Funder", secondary=pub_funder_association, back_populates="publications"
    )

    def last_harvested(self) -> datetime.datetime | None:
        """
        Returns the latest timestamp any source was harvested for this publication.
        """
        timestamps = [
            self.openalex_harvested,
            self.dim_harvested,
            self.sulpub_harvested,
            self.wos_harvested,
            self.pubmed_harvested,
        ]
        valid_timestamps = [t for t in timestamps if t is not None]
        return max(valid_timestamps) if valid_timestamps else None

    def needs_distillation(self) -> bool:
        """
        Returns True if the publication needs to be distilled.
        """
        if self.distilled_at is None:
            return True

        if self.updated_at and self.updated_at > self.distilled_at:
            return True

        return False

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
    sunet: Mapped[str | None] = mapped_column(String, unique=True)
    cap_profile_id: Mapped[str | None] = mapped_column(String, unique=True)
    orcid: Mapped[str | None] = mapped_column(String, unique=True)
    first_name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[bool | None] = mapped_column(Boolean)
    academic_council: Mapped[bool | None] = mapped_column(Boolean)
    role: Mapped[str | None] = mapped_column(String)
    schools: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    departments: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    primary_school: Mapped[str | None] = mapped_column(String)
    primary_dept: Mapped[str | None] = mapped_column(String)
    primary_division: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, default=utcnow(), onupdate=utcnow()
    )
    publications: Mapped[list["Publication"]] = relationship(
        "Publication", secondary=pub_author_association, back_populates="authors"
    )


class Funder(RialtoSchemaBase):
    __tablename__ = "funder"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    grid_id: Mapped[str | None] = mapped_column(String, unique=True)
    ror_id: Mapped[str | None] = mapped_column(String, unique=True)
    openalex_id: Mapped[str | None] = mapped_column(String, unique=True)
    federal: Mapped[bool | None] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, server_default=utcnow()
    )
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, onupdate=utcnow()
    )
    publications: Mapped[list["Publication"]] = relationship(
        "Publication", secondary=pub_funder_association, back_populates="funders"
    )


class Harvest(RialtoSchemaBase):
    __tablename__ = "harvest"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finished_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, server_default=utcnow()
    )
    is_full: Mapped[bool] = mapped_column(Boolean, default=False)

    @classmethod
    def create(cls, is_full=False) -> "Harvest":
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = cls(is_full=is_full)
            session.add(harvest)
            # Sends INSERT now (before commit), so autogenerated fields like id are available.
            session.flush()
            # Returned object is detached (safe to return outside session scope)
            session.expunge(harvest)
            return harvest

    @classmethod
    def get_by_id(cls, harvest_id: int) -> "Harvest":
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = session.get(cls, harvest_id)
            if harvest is None:
                raise ValueError(f"Harvest {harvest_id} not found")
            session.expunge(harvest)
            return harvest

    def get_previous(self) -> "Harvest | None":
        """
        Get the directly preceding harvest using the created_at value.
        If the harvest is a full harvest None will be returned. This ensures
        that harvesting logic does not apply a date limit to the publications
        that are being harvested.
        """

        if self.is_full:
            return None

        with get_session(RIALTO_DB_NAME).begin() as session:
            previous = session.execute(
                select(Harvest)
                .where(Harvest.finished_at.is_not(None))
                .where(Harvest.created_at < self.created_at)
                .order_by(Harvest.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if previous is not None:
                session.expunge(previous)
            return previous

    def complete(self) -> None:
        with get_session(RIALTO_DB_NAME).begin() as session:
            harvest = session.get(Harvest, self.id)
            if harvest is None:
                raise ValueError(f"Harvest {self.id} not found")
            harvest.finished_at = datetime.datetime.now(datetime.UTC)
