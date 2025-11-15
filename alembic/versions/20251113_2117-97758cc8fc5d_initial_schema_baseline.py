"""initial_schema_baseline

Baseline migration that preserves existing lm_metrics table.

This migration establishes the initial schema state for the LogicMonitor
data pipeline. The lm_metrics table already exists in production with
1191+ records, so this migration does NOT create it.

This serves as a baseline for future migrations to build upon.

Revision ID: 97758cc8fc5d
Revises:
Create Date: 2025-11-13 21:17:48.690379

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '97758cc8fc5d'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Upgrade schema - baseline migration.

    The lm_metrics table already exists in production:
    - id SERIAL PRIMARY KEY
    - payload JSONB NOT NULL
    - ingested_at TIMESTAMPTZ DEFAULT NOW()

    This migration only ensures the table exists and does not modify it.
    Running this on an existing database is safe.
    """
    # Check if lm_metrics table exists
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if 'lm_metrics' not in inspector.get_table_names():
        # Create the table only if it doesn't exist
        # This handles fresh database setups
        op.create_table(
            'lm_metrics',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('payload', sa.dialects.postgresql.JSONB(), nullable=False),
            sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=True),
        )


def downgrade() -> None:
    """
    Downgrade schema - baseline migration.

    SAFETY: This migration does NOT drop the lm_metrics table on downgrade
    because it contains production data. Dropping would be destructive.

    To drop the table manually (use with extreme caution):
      DROP TABLE lm_metrics;
    """
    # Do not drop lm_metrics - it contains production data
    # If you need to drop it, do so manually with explicit confirmation
    pass
