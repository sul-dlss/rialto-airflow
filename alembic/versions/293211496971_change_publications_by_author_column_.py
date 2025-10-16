"""Change publications_by_author column types

Revision ID: 293211496971
Revises: 577d7f29ee54
Create Date: 2025-10-15 14:20:25.946633

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "293211496971"
down_revision: Union[str, Sequence[str], None] = "577d7f29ee54"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.alter_column(
        "publications_by_author",
        "academic_council",
        existing_type=sa.String(),
        type_=sa.Boolean(),
        existing_nullable=True,
        nullable=True,
        postgresql_using="academic_council::boolean",
    )
    op.alter_column(
        "publications_by_author",
        "apc",
        existing_type=sa.String(),
        type_=sa.Integer(),
        existing_nullable=True,
        nullable=True,
        postgresql_using="apc::integer",
    )


def downgrade():
    op.alter_column(
        "publications_by_author",
        "academic_council",
        existing_type=sa.Boolean(),
        type_=sa.String(),
        existing_nullable=True,
    )
    op.alter_column(
        "publications_by_author",
        "apc",
        existing_type=sa.Integer(),
        type_=sa.String(),
        existing_nullable=True,
    )
