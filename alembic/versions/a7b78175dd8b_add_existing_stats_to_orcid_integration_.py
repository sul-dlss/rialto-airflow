"""Add existing stats to orcid_integration_stats

Revision ID: a7b78175dd8b
Revises: ad15040c0085
Create Date: 2025-09-10 10:27:27.060148

"""

import os
from typing import Sequence, Union

from alembic import op
import pandas as pd
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a7b78175dd8b"
down_revision: Union[str, Sequence[str], None] = "ad15040c0085"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Get the seed (historic) orcid stats data
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_filepath = os.path.join(
        os.path.dirname(current_dir), "orcid-integration-stats-seed.csv"
    )

    # turn CSV into a list of dicts for bulk insert
    df = pd.read_csv(csv_filepath)
    orcid_stats = df.to_dict(orient="records")

    op.bulk_insert(
        sa.table(
            "orcid_integration_stats",
            sa.column("date_label", sa.String),
            sa.column("read_only_scope", sa.Integer),
            sa.column("read_write_scope", sa.Integer),
        ),
        orcid_stats,
    )


def downgrade():
    # Delete all rows from the orcid_integration_stats table
    op.execute("DELETE FROM orcid_integration_stats;")
