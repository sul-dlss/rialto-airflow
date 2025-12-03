"""Drop timestamp columns

Revision ID: ef9157a6df79
Revises: 148d276f0f9c
Create Date: 2025-12-02 16:21:58.554124

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from rialto_airflow.database import utcnow


# revision identifiers, used by Alembic.
revision: str = "ef9157a6df79"
down_revision: Union[str, Sequence[str], None] = "148d276f0f9c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column("publications", "created_at")
    op.drop_column("publications", "updated_at")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "publications",
        sa.Column("created_at", sa.DateTime, server_default=utcnow()),
    )
    op.add_column(
        "publications",
        sa.Column("updated_at", sa.DateTime, onupdate=utcnow()),
    )
