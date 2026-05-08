"""add distilled_at to publication

Revision ID: 5464c1bc83f7
Revises: e1d3bfbd9ffd
Create Date: 2026-05-08 15:04:06.362364

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5464c1bc83f7'
down_revision: Union[str, Sequence[str], None] = 'e1d3bfbd9ffd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('publication', sa.Column('distilled_at', sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('publication', 'distilled_at')
