"""Add existing stats to orcid_integration_stats

Revision ID: a7b78175dd8b
Revises: ad15040c0085
Create Date: 2025-09-10 10:27:27.060148

"""
import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a7b78175dd8b"
down_revision: Union[str, Sequence[str], None] = "ad15040c0085"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Get the directory of the current migration file
    dirname = os.path.dirname(__file__).parent
    # Construct the path to your CSV file (assuming it's relative to the migration file)
    csv_filepath = os.path.join(dirname, 'orcid-integration-stats-seed.csv')

    # Execute the COPY command to load data from the CSV
    op.execute(f"COPY orcid_integration_stats FROM '{csv_filepath}' WITH (FORMAT CSV, HEADER TRUE);")


def downgrade():
    # Delete all rows from the orcid_integration_stats table
    op.execute("DELETE FROM orcid_integration_stats;")


