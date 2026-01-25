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

    # Step 2: Drop materialized views that depend on metric_data
    # They will be recreated after the partitioned table is set up
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_metrics CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS hourly_aggregates CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS resource_summary CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS datasource_metrics CASCADE")

    # Step 3: Drop existing foreign key constraints on metric_data
    op.drop_constraint('fk_metric_data_resource', 'metric_data', type_='foreignkey')
    op.drop_constraint('fk_metric_data_metric_definition', 'metric_data', type_='foreignkey')

    # Step 4: Drop existing indexes
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
                end_date DATE := DATE '{max_month.strftime('%Y-%m-%d')}' + INTERVAL '2 months';
                curr_dt DATE := start_date;
                partition_name TEXT;
                partition_start TEXT;
                partition_end TEXT;
            BEGIN
                WHILE curr_dt < end_date LOOP
                    partition_name := 'metric_data_' || TO_CHAR(curr_dt, 'YYYY_MM');
                    partition_start := TO_CHAR(curr_dt, 'YYYY-MM-01');
                    partition_end := TO_CHAR(curr_dt + INTERVAL '1 month', 'YYYY-MM-01');

                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_data
                         FOR VALUES FROM (%L) TO (%L)',
                        partition_name, partition_start, partition_end
                    );

                    curr_dt := curr_dt + INTERVAL '1 month';
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
                curr_dt DATE := start_date;
                partition_name TEXT;
                partition_start TEXT;
                partition_end TEXT;
            BEGIN
                WHILE curr_dt < end_date LOOP
                    partition_name := 'metric_data_' || TO_CHAR(curr_dt, 'YYYY_MM');
                    partition_start := TO_CHAR(curr_dt, 'YYYY-MM-01');
                    partition_end := TO_CHAR(curr_dt + INTERVAL '1 month', 'YYYY-MM-01');

                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_data
                         FOR VALUES FROM (%L) TO (%L)',
                        partition_name, partition_start, partition_end
                    );

                    curr_dt := curr_dt + INTERVAL '1 month';
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

    # Step 13: Recreate materialized views
    op.execute("""
        CREATE MATERIALIZED VIEW latest_metrics AS
        SELECT DISTINCT ON (md.resource_id, md.metric_definition_id)
            md.id as metric_data_id,
            md.resource_id,
            r.resource_hash,
            r.attributes as resource_attributes,
            md.metric_definition_id,
            mdef.name as metric_name,
            mdef.unit as metric_unit,
            mdef.metric_type,
            ds.name as datasource_name,
            ds.version as datasource_version,
            md.timestamp,
            md.value_double,
            md.value_int,
            md.attributes as metric_attributes
        FROM metric_data md
        JOIN resources r ON md.resource_id = r.id
        JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
        JOIN datasources ds ON mdef.datasource_id = ds.id
        ORDER BY md.resource_id, md.metric_definition_id, md.timestamp DESC
    """)
    op.execute("CREATE INDEX ix_latest_metrics_resource_id ON latest_metrics(resource_id)")
    op.execute("CREATE INDEX ix_latest_metrics_metric_definition_id ON latest_metrics(metric_definition_id)")
    op.execute("CREATE INDEX ix_latest_metrics_datasource_name ON latest_metrics(datasource_name)")
    op.execute("CREATE INDEX ix_latest_metrics_timestamp ON latest_metrics(timestamp DESC)")

    op.execute("""
        CREATE MATERIALIZED VIEW hourly_aggregates AS
        SELECT
            md.resource_id,
            r.resource_hash,
            md.metric_definition_id,
            mdef.name as metric_name,
            mdef.unit as metric_unit,
            ds.name as datasource_name,
            ds.version as datasource_version,
            DATE_TRUNC('hour', md.timestamp) as hour,
            COUNT(*) as data_point_count,
            MIN(COALESCE(md.value_double, md.value_int::float)) as min_value,
            MAX(COALESCE(md.value_double, md.value_int::float)) as max_value,
            AVG(COALESCE(md.value_double, md.value_int::float)) as avg_value,
            STDDEV(COALESCE(md.value_double, md.value_int::float)) as stddev_value
        FROM metric_data md
        JOIN resources r ON md.resource_id = r.id
        JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
        JOIN datasources ds ON mdef.datasource_id = ds.id
        GROUP BY
            md.resource_id, r.resource_hash, md.metric_definition_id,
            mdef.name, mdef.unit, ds.name, ds.version,
            DATE_TRUNC('hour', md.timestamp)
    """)
    op.execute("CREATE INDEX ix_hourly_aggregates_hour ON hourly_aggregates(hour DESC)")
    op.execute("CREATE INDEX ix_hourly_aggregates_resource_metric ON hourly_aggregates(resource_id, metric_definition_id, hour DESC)")
    op.execute("CREATE INDEX ix_hourly_aggregates_datasource ON hourly_aggregates(datasource_name, hour DESC)")

    op.execute("""
        CREATE MATERIALIZED VIEW resource_summary AS
        SELECT
            r.id as resource_id,
            r.resource_hash,
            r.attributes,
            r.created_at,
            r.updated_at,
            COUNT(DISTINCT md.metric_definition_id) as metric_count,
            COUNT(md.id) as total_data_points,
            MAX(md.timestamp) as last_metric_timestamp,
            MIN(md.timestamp) as first_metric_timestamp,
            ARRAY_AGG(DISTINCT ds.name) as datasource_names
        FROM resources r
        LEFT JOIN metric_data md ON r.id = md.resource_id
        LEFT JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
        LEFT JOIN datasources ds ON mdef.datasource_id = ds.id
        GROUP BY r.id, r.resource_hash, r.attributes, r.created_at, r.updated_at
    """)
    op.execute("CREATE INDEX ix_resource_summary_resource_hash ON resource_summary(resource_hash)")
    op.execute("CREATE INDEX ix_resource_summary_last_metric_timestamp ON resource_summary(last_metric_timestamp DESC)")
    op.execute("CREATE INDEX ix_resource_summary_metric_count ON resource_summary(metric_count DESC)")

    op.execute("""
        CREATE MATERIALIZED VIEW datasource_metrics AS
        SELECT
            ds.id as datasource_id,
            ds.name as datasource_name,
            ds.version as datasource_version,
            ds.created_at as datasource_created_at,
            mdef.id as metric_definition_id,
            mdef.name as metric_name,
            mdef.unit as metric_unit,
            mdef.metric_type,
            mdef.description as metric_description,
            COUNT(DISTINCT md.resource_id) as resource_count,
            COUNT(md.id) as total_data_points,
            MAX(md.timestamp) as last_data_point_timestamp,
            MIN(md.timestamp) as first_data_point_timestamp
        FROM datasources ds
        JOIN metric_definitions mdef ON ds.id = mdef.datasource_id
        LEFT JOIN metric_data md ON mdef.id = md.metric_definition_id
        GROUP BY
            ds.id, ds.name, ds.version, ds.created_at,
            mdef.id, mdef.name, mdef.unit, mdef.metric_type, mdef.description
    """)
    op.execute("CREATE INDEX ix_datasource_metrics_datasource_name ON datasource_metrics(datasource_name)")
    op.execute("CREATE INDEX ix_datasource_metrics_metric_name ON datasource_metrics(metric_name)")


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
