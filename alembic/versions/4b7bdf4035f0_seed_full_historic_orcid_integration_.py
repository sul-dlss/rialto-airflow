"""Seed full historic orcid integration stats

Revision ID: 4b7bdf4035f0
Revises: a7b78175dd8b
Create Date: 2025-09-16 10:37:29.794892

"""

import os
from typing import Sequence, Union

from alembic import op
import pandas as pd
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4b7bdf4035f0"
down_revision: Union[str, Sequence[str], None] = "a7b78175dd8b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Delete the existing data
    op.execute("DELETE FROM orcid_integration_stats;")

    # Get CSV with the full history of the orcid stats up 8/5/2021-9/14/2025
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
        # mypy expects keys in the dict to be type string, but the result of the df.to_dict() will have keys with type Hashable.
        # This is known data, so not worrying about it.
        orcid_stats,  # type: ignore
    )


def downgrade():
    # Delete all rows from the orcid_integration_stats table
    op.execute("DELETE FROM orcid_integration_stats;")
