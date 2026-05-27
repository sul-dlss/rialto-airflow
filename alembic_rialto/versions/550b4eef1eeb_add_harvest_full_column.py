"""add harvest.full column

Revision ID: 550b4eef1eeb
Revises: 5464c1bc83f7
Create Date: 2026-05-26 12:52:52.564722

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "550b4eef1eeb"
down_revision: Union[str, Sequence[str], None] = "5464c1bc83f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "harvest",
        sa.Column("is_full", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("harvest", "is_full")
