# Description: Migration to partition metric_data table by timestamp.
# Description: Uses PostgreSQL declarative partitioning for improved query performance.

"""partition_metric_data_by_timestamp

Partitions the metric_data table by timestamp using PostgreSQL's declarative
partitioning. This improves query performance for time-range queries and
allows for efficient data lifecycle management.

Strategy:
1. Create new partitioned table with same schema
2. Create monthly partitions for existing data range
3. Copy data from old table to partitioned table
4. Swap tables atomically
5. Recreate foreign keys and indexes

Revision ID: 9eafd921251d
Revises: 6f6f6a0c623f
Create Date: 2026-01-24 20:42:07.931659

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '9eafd921251d'
down_revision: Union[str, Sequence[str], None] = '6f6f6a0c623f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Convert metric_data to a partitioned table.

    This migration:
    1. Creates a new partitioned table metric_data_partitioned
    2. Creates monthly partitions based on existing data range
    3. Copies all data from metric_data to partitioned table
    4. Drops old table and renames new one
    5. Recreates foreign keys and indexes
    """
    conn = op.get_bind()

    # Step 1: Get the date range of existing data to create partitions
    result = conn.execute(sa.text("""
        SELECT
            DATE_TRUNC('month', MIN(timestamp)) as min_month,
            DATE_TRUNC('month', MAX(timestamp)) as max_month
        FROM metric_data
    """))
    row = result.fetchone()
    min_month = row[0] if row and row[0] else None
    max_month = row[1] if row and row[1] else None

    # Step 2: Drop existing foreign key constraints on metric_data
    op.drop_constraint('fk_metric_data_resource', 'metric_data', type_='foreignkey')
    op.drop_constraint('fk_metric_data_metric_definition', 'metric_data', type_='foreignkey')

    # Step 3: Drop existing indexes
    op.drop_index('ix_metric_data_timestamp_desc', 'metric_data')
    op.drop_index('ix_metric_data_resource_metric_time', 'metric_data')
    op.drop_index('ix_metric_data_metric_time', 'metric_data')
    op.drop_index('ix_metric_data_resource_time', 'metric_data')
    op.drop_index('ix_metric_data_attributes', 'metric_data')

    # Step 4: Rename old table
    op.rename_table('metric_data', 'metric_data_old')

    # Step 5: Create new partitioned table
    op.execute("""
        CREATE TABLE metric_data (
            id BIGSERIAL,
            resource_id INTEGER NOT NULL,
            metric_definition_id INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            value_double DOUBLE PRECISION,
            value_int BIGINT,
            attributes JSONB,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id, timestamp)
        ) PARTITION BY RANGE (timestamp)
    """)

    # Add comment
    op.execute("""
        COMMENT ON TABLE metric_data IS 'Time-series metric data points (partitioned by month)'
    """)

    # Step 6: Create partitions for existing data range
    if min_month and max_month:
        # Generate monthly partitions from min_month to max_month + 2 months
        op.execute(f"""
            DO $$
            DECLARE
                start_date DATE := '{min_month.strftime('%Y-%m-%d')}';
                end_date DATE := '{max_month.strftime('%Y-%m-%d')}' + INTERVAL '2 months';
                current_date DATE := start_date;
                partition_name TEXT;
                partition_start TEXT;
                partition_end TEXT;
            BEGIN
                WHILE current_date < end_date LOOP
                    partition_name := 'metric_data_' || TO_CHAR(current_date, 'YYYY_MM');
                    partition_start := TO_CHAR(current_date, 'YYYY-MM-01');
                    partition_end := TO_CHAR(current_date + INTERVAL '1 month', 'YYYY-MM-01');

                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_data
                         FOR VALUES FROM (%L) TO (%L)',
                        partition_name, partition_start, partition_end
                    );

                    current_date := current_date + INTERVAL '1 month';
                END LOOP;
            END $$;
        """)
    else:
        # No data - create partitions for current and next 2 months
        op.execute("""
            DO $$
            DECLARE
                start_date DATE := DATE_TRUNC('month', CURRENT_DATE);
                end_date DATE := start_date + INTERVAL '3 months';
                current_date DATE := start_date;
                partition_name TEXT;
                partition_start TEXT;
                partition_end TEXT;
            BEGIN
                WHILE current_date < end_date LOOP
                    partition_name := 'metric_data_' || TO_CHAR(current_date, 'YYYY_MM');
                    partition_start := TO_CHAR(current_date, 'YYYY-MM-01');
                    partition_end := TO_CHAR(current_date + INTERVAL '1 month', 'YYYY-MM-01');

                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_data
                         FOR VALUES FROM (%L) TO (%L)',
                        partition_name, partition_start, partition_end
                    );

                    current_date := current_date + INTERVAL '1 month';
                END LOOP;
            END $$;
        """)

    # Step 7: Create a default partition for data outside defined ranges
    op.execute("""
        CREATE TABLE IF NOT EXISTS metric_data_default
        PARTITION OF metric_data DEFAULT
    """)

    # Step 8: Copy data from old table to new partitioned table
    op.execute("""
        INSERT INTO metric_data (id, resource_id, metric_definition_id, timestamp,
                                  value_double, value_int, attributes, created_at)
        SELECT id, resource_id, metric_definition_id, timestamp,
               value_double, value_int, attributes, created_at
        FROM metric_data_old
    """)

    # Step 9: Reset the sequence to continue from max id
    op.execute("""
        SELECT setval('metric_data_id_seq',
                      COALESCE((SELECT MAX(id) FROM metric_data), 1))
    """)

    # Step 10: Add foreign key constraints
    op.create_foreign_key(
        'fk_metric_data_resource',
        'metric_data',
        'resources',
        ['resource_id'],
        ['id'],
        ondelete='CASCADE'
    )

    op.create_foreign_key(
        'fk_metric_data_metric_definition',
        'metric_data',
        'metric_definitions',
        ['metric_definition_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # Step 11: Create indexes on parent table (inherited by partitions)
    op.create_index(
        'ix_metric_data_timestamp_desc',
        'metric_data',
        [sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_resource_metric_time',
        'metric_data',
        ['resource_id', 'metric_definition_id', sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_metric_time',
        'metric_data',
        ['metric_definition_id', sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_resource_time',
        'metric_data',
        ['resource_id', sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_attributes',
        'metric_data',
        ['attributes'],
        postgresql_using='gin'
    )

    # Step 12: Drop old table (data has been copied)
    op.drop_table('metric_data_old')


def downgrade() -> None:
    """
    Convert back to non-partitioned table.

    Note: This is a destructive operation that recreates the table.
    All partition structure will be lost.
    """
    conn = op.get_bind()

    # Step 1: Drop foreign key constraints
    op.drop_constraint('fk_metric_data_resource', 'metric_data', type_='foreignkey')
    op.drop_constraint('fk_metric_data_metric_definition', 'metric_data', type_='foreignkey')

    # Step 2: Drop indexes
    op.drop_index('ix_metric_data_timestamp_desc', 'metric_data')
    op.drop_index('ix_metric_data_resource_metric_time', 'metric_data')
    op.drop_index('ix_metric_data_metric_time', 'metric_data')
    op.drop_index('ix_metric_data_resource_time', 'metric_data')
    op.drop_index('ix_metric_data_attributes', 'metric_data')

    # Step 3: Create temporary table to hold data
    op.execute("""
        CREATE TABLE metric_data_temp AS
        SELECT * FROM metric_data
    """)

    # Step 4: Drop partitioned table (cascades to partitions)
    op.drop_table('metric_data')

    # Step 5: Create non-partitioned table
    op.create_table(
        'metric_data',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('resource_id', sa.Integer(), nullable=False),
        sa.Column('metric_definition_id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('value_double', sa.Float(), nullable=True),
        sa.Column('value_int', sa.BigInteger(), nullable=True),
        sa.Column('attributes', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True),
                  server_default=sa.text('NOW()'), nullable=False),
    )

    # Step 6: Copy data back
    op.execute("""
        INSERT INTO metric_data (id, resource_id, metric_definition_id, timestamp,
                                  value_double, value_int, attributes, created_at)
        SELECT id, resource_id, metric_definition_id, timestamp,
               value_double, value_int, attributes, created_at
        FROM metric_data_temp
    """)

    # Step 7: Reset sequence
    op.execute("""
        SELECT setval('metric_data_id_seq',
                      COALESCE((SELECT MAX(id) FROM metric_data), 1))
    """)

    # Step 8: Drop temp table
    op.drop_table('metric_data_temp')

    # Step 9: Recreate foreign keys
    op.create_foreign_key(
        'fk_metric_data_resource',
        'metric_data',
        'resources',
        ['resource_id'],
        ['id'],
        ondelete='CASCADE'
    )

    op.create_foreign_key(
        'fk_metric_data_metric_definition',
        'metric_data',
        'metric_definitions',
        ['metric_definition_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # Step 10: Recreate indexes
    op.create_index(
        'ix_metric_data_timestamp_desc',
        'metric_data',
        [sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_resource_metric_time',
        'metric_data',
        ['resource_id', 'metric_definition_id', sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_metric_time',
        'metric_data',
        ['metric_definition_id', sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_resource_time',
        'metric_data',
        ['resource_id', sa.text('timestamp DESC')]
    )

    op.create_index(
        'ix_metric_data_attributes',
        'metric_data',
        ['attributes'],
        postgresql_using='gin'
    )
