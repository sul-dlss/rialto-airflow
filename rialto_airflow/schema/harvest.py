from sqlalchemy import Table, Boolean, Column, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import (  # type: ignore
    RelationshipProperty,
    declarative_base,
    relationship,
)
from sqlalchemy.types import DateTime

from rialto_airflow.database import utcnow


HarvestSchemaBase = declarative_base()

pub_author_association = Table(
    "pub_author_association",
    HarvestSchemaBase.metadata,
    Column(
        "publication_id",
        ForeignKey("publication.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("author_id", ForeignKey("author.id", ondelete="CASCADE"), primary_key=True),
)


pub_funder_association = Table(
    "pub_funder_association",
    HarvestSchemaBase.metadata,
    Column("publication_id", ForeignKey("publication.id"), primary_key=True),
    Column("funder_id", ForeignKey("funder.id"), primary_key=True),
)


class Publication(HarvestSchemaBase):  # type: ignore
    __tablename__ = "publication"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doi = Column(String, unique=True)
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
    types = Column(ARRAY(String))
    publisher = Column(String)
    authors: RelationshipProperty = relationship(
        "Author",
        secondary=pub_author_association,
        back_populates="publications",
        cascade="all, delete",
    )
    funders: RelationshipProperty = relationship(
        "Funder", secondary=pub_funder_association, back_populates="publications"
    )

    __table_args__ = (
        Index("idx_openalex_id", text("(openalex_json->>'id')")),
        Index("idx_wos_id", text("(wos_json->>'UID')")),
        Index("idx_sulpub_id", text("(sulpub_json->>'sulpubid')")),
        Index("idx_dim_id", text("(dim_json->>'id')")),
    )


class Author(HarvestSchemaBase):  # type: ignore
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


class Funder(HarvestSchemaBase):  # type: ignore
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
