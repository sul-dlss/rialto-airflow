from sqlalchemy import Boolean, Column, Integer, String
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
