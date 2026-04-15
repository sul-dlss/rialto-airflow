"""Add title and author list names to publications

Revision ID: 292d33c40c89
Revises: ef9157a6df79
Create Date: 2026-04-15 19:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "292d33c40c89"
down_revision: Union[str, Sequence[str], None] = "ef9157a6df79"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "publications", sa.Column("author_list_names", sa.Text(), nullable=True)
    )
    op.add_column("publications", sa.Column("title", sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("publications", "title")
    op.drop_column("publications", "author_list_names")
