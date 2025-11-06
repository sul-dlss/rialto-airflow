"""Drop ISSN column from reports

Revision ID: b832bef1963f
Revises: 85fa8f2e6ae5
Create Date: 2025-11-06 07:44:17.369585

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b832bef1963f"
down_revision: Union[str, Sequence[str], None] = "85fa8f2e6ae5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column("publications_by_author", "journal_issn")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "publications_by_author", sa.Column("journal_issn", sa.String(), nullable=True)
    )
