"""Remove funder_list columns from publication_by_authors

Revision ID: 148d276f0f9c
Revises: b832bef1963f
Create Date: 2025-12-01 14:26:45.501111

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "148d276f0f9c"
down_revision: Union[str, Sequence[str], None] = "b832bef1963f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column("publications_by_author", "funder_list_grid")
    op.drop_column("publications_by_author", "funder_list_name")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "publications_by_author",
        sa.Column("funder_list_grid", sa.Text(), nullable=True),
    )
    op.add_column(
        "publications_by_author",
        sa.Column("funder_list_name", sa.Text(), nullable=True),
    )
