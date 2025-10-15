"""Create publications_by_author table

Revision ID: 577d7f29ee54
Revises: 77f8f9b13935
Create Date: 2025-10-13 14:39:07.840563

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "577d7f29ee54"
down_revision: Union[str, Sequence[str], None] = "77f8f9b13935"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "publications_by_author",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("doi", sa.String, nullable=True),
        sa.Column("sunet", sa.String, nullable=False),
        sa.Column("orcid", sa.String, nullable=True),
        sa.Column("abstract", sa.Text, nullable=True),
        sa.Column("academic_council", sa.String, nullable=True),
        sa.Column("apc", sa.String, nullable=True),
        sa.Column("author_list_names", sa.Text, nullable=True),
        sa.Column("author_list_orcids", sa.Text, nullable=True),
        sa.Column("citation_count", sa.Integer, nullable=True),
        sa.Column("federally_funded", sa.Boolean, nullable=True),
        sa.Column("first_author_name", sa.String, nullable=True),
        sa.Column("first_author_orcid", sa.String, nullable=True),
        sa.Column("funder_list_grid", sa.Text, nullable=True),
        sa.Column("funder_list_name", sa.Text, nullable=True),
        sa.Column("grant_ids", sa.Text, nullable=True),
        sa.Column("issue", sa.String, nullable=True),
        sa.Column("journal_issn", sa.String, nullable=True),
        sa.Column("journal_name", sa.String, nullable=True),
        sa.Column("last_author_name", sa.String, nullable=True),
        sa.Column("last_author_orcid", sa.String, nullable=True),
        sa.Column("open_access", sa.String, nullable=True),
        sa.Column("pages", sa.String, nullable=True),
        sa.Column("primary_department", sa.String, nullable=True),
        sa.Column("primary_school", sa.String, nullable=True),
        sa.Column("pub_year", sa.Integer, nullable=True),
        sa.Column("publisher", sa.String, nullable=True),
        sa.Column("role", sa.String, nullable=True),
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("types", sa.String, nullable=True),
        sa.Column("volume", sa.String, nullable=True),
        sa.UniqueConstraint("doi", "sunet", name="uq_publications_by_author_doi_sunet"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("publications_by_author")
