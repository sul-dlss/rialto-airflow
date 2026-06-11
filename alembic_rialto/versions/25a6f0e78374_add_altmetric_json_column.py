"""add altmetric_json column

Revision ID: 25a6f0e78374
Revises: 550b4eef1eeb
Create Date: 2026-06-05 18:26:15.818820

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "25a6f0e78374"
down_revision: Union[str, Sequence[str], None] = "550b4eef1eeb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add altmetric_json column"""
    op.add_column(
        "publication",
        sa.Column(
            "altmetric_json",
            postgresql.JSONB(none_as_null=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Remove altmetric_json column."""
    op.drop_column("publication", "altmetric_json")
