"""initialize reports DB

Revision ID: f47e769bc111
Revises:
Create Date: 2025-09-04 00:39:22.624265

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from rialto_airflow.database import utcnow


# revision identifiers, used by Alembic.
revision: str = "f47e769bc111"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "publications",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("doi", sa.String, unique=True),
        sa.Column("pub_year", sa.Integer),
        sa.Column("apc", sa.Integer),
        sa.Column("open_access", sa.String),
        sa.Column("types", sa.String),
        sa.Column("federally_funded", sa.Boolean),
        sa.Column("academic_council_authored", sa.Boolean),
        sa.Column("faculty_authored", sa.Boolean),
        sa.Column("created_at", sa.DateTime, server_default=utcnow()),
        sa.Column("updated_at", sa.DateTime, onupdate=utcnow()),
        if_not_exists=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("publications")
