"""cascade delete pub_funder_association

Revision ID: e1d3bfbd9ffd
Revises: ad881a79ea4d
Create Date: 2026-05-01

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e1d3bfbd9ffd"
down_revision: Union[str, Sequence[str], None] = "ad881a79ea4d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        "pub_funder_association_publication_id_fkey",
        "pub_funder_association",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "pub_funder_association_publication_id_fkey",
        "pub_funder_association",
        "publication",
        ["publication_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "pub_funder_association_publication_id_fkey",
        "pub_funder_association",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "pub_funder_association_publication_id_fkey",
        "pub_funder_association",
        "publication",
        ["publication_id"],
        ["id"],
    )
