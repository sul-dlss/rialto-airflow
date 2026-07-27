"""Remove full_name column from author_orcids

Revision ID: b9d9af22500b
Revises: 293211496971
Create Date: 2025-10-17 07:59:14.670070

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b9d9af22500b"
down_revision: str | Sequence[str] | None = "293211496971"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("author_orcids", "full_name")


def downgrade() -> None:
    op.add_column("author_orcids", sa.Column("full_name", sa.String(), nullable=True))
