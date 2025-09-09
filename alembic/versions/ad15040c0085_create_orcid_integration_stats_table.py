"""create orcid_integration_stats table

Revision ID: ad15040c0085
Revises: 08f49e63532b
Create Date: 2025-09-09 15:47:22.145019

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ad15040c0085'
down_revision: Union[str, Sequence[str], None] = '08f49e63532b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
       "orcid_integration_stats",
       sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
       sa.Column("date_label", sa.String),
       sa.Column("read_only_scope", sa.Integer),
       sa.Column("read_write_scope", sa.Integer)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("orcid_integration_stats")
