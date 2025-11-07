"""Add journal name to publications

Revision ID: 85fa8f2e6ae5
Revises: 34bd25e2696c
Create Date: 2025-11-05 13:18:02.348739

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "85fa8f2e6ae5"
down_revision: Union[str, Sequence[str], None] = "34bd25e2696c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("publications", sa.Column("journal_name", sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("publications", "journal_name")
