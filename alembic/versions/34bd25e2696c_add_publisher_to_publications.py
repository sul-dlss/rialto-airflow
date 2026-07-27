"""Add publisher to publications

Revision ID: 34bd25e2696c
Revises: b9d9af22500b
Create Date: 2025-10-22 11:20:56.953180

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "34bd25e2696c"
down_revision: str | Sequence[str] | None = "b9d9af22500b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("publications", sa.Column("publisher", sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("publications", "publisher")
