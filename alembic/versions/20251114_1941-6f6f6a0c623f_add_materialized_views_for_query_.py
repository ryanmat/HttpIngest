"""add_materialized_views_for_query_optimization

Revision ID: 6f6f6a0c623f
Revises: fb9b24da90b7
Create Date: 2025-11-14 19:41:22.175276

Adds materialized views for common query patterns:
- latest_metrics: Most recent value for each metric per resource
- hourly_aggregates: Min/max/avg/count per hour for each metric
- resource_summary: Metrics count and last update per resource
- datasource_metrics: All metrics for each datasource with descriptions

Includes indexes and refresh functions for optimal performance.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "6f6f6a0c623f"
down_revision: Union[str, Sequence[str], None] = "fb9b24da90b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add materialized views for query optimization."""

    # 1. LATEST_METRICS VIEW
    # Most recent value for each metric per resource
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

    # Index for latest_metrics
    op.execute(
        "CREATE INDEX ix_latest_metrics_resource_id ON latest_metrics(resource_id)"
    )
    op.execute(
        "CREATE INDEX ix_latest_metrics_metric_definition_id ON latest_metrics(metric_definition_id)"
    )
    op.execute(
        "CREATE INDEX ix_latest_metrics_datasource_name ON latest_metrics(datasource_name)"
    )
    op.execute(
        "CREATE INDEX ix_latest_metrics_timestamp ON latest_metrics(timestamp DESC)"
    )

    # 2. HOURLY_AGGREGATES VIEW
    # Min/max/avg/count per hour for each metric
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
            md.resource_id,
            r.resource_hash,
            md.metric_definition_id,
            mdef.name,
            mdef.unit,
            ds.name,
            ds.version,
            DATE_TRUNC('hour', md.timestamp)
    """)

    # Indexes for hourly_aggregates
    op.execute(
        "CREATE INDEX ix_hourly_aggregates_hour ON hourly_aggregates(hour DESC)"
    )
    op.execute(
        "CREATE INDEX ix_hourly_aggregates_resource_metric ON hourly_aggregates(resource_id, metric_definition_id, hour DESC)"
    )
    op.execute(
        "CREATE INDEX ix_hourly_aggregates_datasource ON hourly_aggregates(datasource_name, hour DESC)"
    )

    # 3. RESOURCE_SUMMARY VIEW
    # Metrics count and last update per resource
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

    # Indexes for resource_summary
    op.execute(
        "CREATE INDEX ix_resource_summary_resource_hash ON resource_summary(resource_hash)"
    )
    op.execute(
        "CREATE INDEX ix_resource_summary_last_metric_timestamp ON resource_summary(last_metric_timestamp DESC)"
    )
    op.execute(
        "CREATE INDEX ix_resource_summary_metric_count ON resource_summary(metric_count DESC)"
    )

    # 4. DATASOURCE_METRICS VIEW
    # All metrics for each datasource with descriptions
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
            ds.id,
            ds.name,
            ds.version,
            ds.created_at,
            mdef.id,
            mdef.name,
            mdef.unit,
            mdef.metric_type,
            mdef.description
    """)

    # Indexes for datasource_metrics
    op.execute(
        "CREATE INDEX ix_datasource_metrics_datasource_name ON datasource_metrics(datasource_name)"
    )
    op.execute(
        "CREATE INDEX ix_datasource_metrics_metric_name ON datasource_metrics(metric_name)"
    )
    op.execute(
        "CREATE INDEX ix_datasource_metrics_resource_count ON datasource_metrics(resource_count DESC)"
    )

    # Create refresh function for all materialized views
    op.execute("""
        CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW CONCURRENTLY latest_metrics;
            REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_aggregates;
            REFRESH MATERIALIZED VIEW CONCURRENTLY resource_summary;
            REFRESH MATERIALIZED VIEW CONCURRENTLY datasource_metrics;
        END;
        $$;
    """)

    # Create individual refresh functions for selective refresh
    op.execute("""
        CREATE OR REPLACE FUNCTION refresh_latest_metrics()
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW CONCURRENTLY latest_metrics;
        END;
        $$;
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION refresh_hourly_aggregates()
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_aggregates;
        END;
        $$;
    """)


def downgrade() -> None:
    """Downgrade schema - Remove materialized views."""

    # Drop refresh functions
    op.execute("DROP FUNCTION IF EXISTS refresh_all_materialized_views()")
    op.execute("DROP FUNCTION IF EXISTS refresh_latest_metrics()")
    op.execute("DROP FUNCTION IF EXISTS refresh_hourly_aggregates()")

    # Drop materialized views (indexes are dropped automatically)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS datasource_metrics")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS resource_summary")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS hourly_aggregates")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_metrics")
