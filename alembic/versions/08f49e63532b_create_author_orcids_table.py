"""create author_orcids table

Revision ID: 08f49e63532b
Revises: f47e769bc111
Create Date: 2025-09-09 15:37:36.196839

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from rialto_airflow.database import utcnow


# revision identifiers, used by Alembic.
revision: str = "08f49e63532b"
down_revision: Union[str, Sequence[str], None] = "f47e769bc111"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "author_orcids",
        sa.Column("orcidid", sa.String, primary_key=True),
        sa.Column("sunetid", sa.String),
        sa.Column("full_name", sa.String),
        sa.Column("orcid_update_scope", sa.Boolean),
        sa.Column("role", sa.String),
        sa.Column("primary_affiliation", sa.String),
        sa.Column("primary_school", sa.String),
        sa.Column("primary_department", sa.String),
        sa.Column("primary_division", sa.String),
        sa.Column("created_at", sa.DateTime, server_default=utcnow()),
        sa.Column("updated_at", sa.DateTime, onupdate=utcnow()),
        if_not_exists=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("author_orcids")
