"""Add first and last name to author_orcids

Revision ID: 77f8f9b13935
Revises: d8e2297c89bb
Create Date: 2025-10-13 08:32:08.108992

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "77f8f9b13935"
down_revision: Union[str, Sequence[str], None] = "d8e2297c89bb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column("author_orcids", sa.Column("first_name", sa.String(), nullable=True))
    op.add_column("author_orcids", sa.Column("last_name", sa.String(), nullable=True))


def downgrade():
    op.drop_column("author_orcids", "first_name")
    op.drop_column("author_orcids", "last_name")
