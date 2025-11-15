"""add_normalized_otlp_tables

Creates normalized tables for OTLP data storage and processing.

This migration adds five tables to store parsed and normalized OTLP metrics:
1. resources - Device/service information from OTLP resource attributes
2. datasources - LogicMonitor datasource metadata from OTLP scopes
3. metric_definitions - Metric names, types, and units
4. metric_data - Time-series metric values with proper indexing
5. processing_status - Tracks which lm_metrics records have been processed

Includes optimized indexes for time-series queries and foreign key constraints.

Revision ID: fb9b24da90b7
Revises: 97758cc8fc5d
Create Date: 2025-11-13 21:27:18.904527

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'fb9b24da90b7'
down_revision: Union[str, Sequence[str], None] = '97758cc8fc5d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Create normalized OTLP tables.

    Tables are created in dependency order to satisfy foreign key constraints.
    """
    # 1. RESOURCES TABLE
    # Stores device/service information from OTLP resource.attributes
    op.create_table(
        'resources',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('resource_hash', sa.String(64), nullable=False, unique=True,
                  comment='SHA256 hash of sorted resource attributes for deduplication'),
        sa.Column('attributes', postgresql.JSONB(), nullable=False,
                  comment='Full resource attributes from OTLP (service.name, host.name, etc)'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        comment='Device and service information from OTLP resource attributes'
    )

    # Index for attribute queries (GIN index for JSONB)
    op.create_index(
        'ix_resources_attributes',
        'resources',
        ['attributes'],
        postgresql_using='gin'
    )

    # 2. DATASOURCES TABLE
    # Stores LogicMonitor datasource metadata from OTLP scope
    op.create_table(
        'datasources',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('name', sa.String(255), nullable=False,
                  comment='Datasource name (e.g., CPU_Usage, Memory_Usage)'),
        sa.Column('version', sa.String(50), nullable=True,
                  comment='Datasource version from OTLP scope'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        comment='LogicMonitor datasource metadata'
    )

    # Unique constraint on name+version combination
    op.create_index(
        'ix_datasources_name_version',
        'datasources',
        ['name', 'version'],
        unique=True
    )

    # 3. METRIC_DEFINITIONS TABLE
    # Stores metric names, types, and units
    op.create_table(
        'metric_definitions',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('datasource_id', sa.Integer(), nullable=False,
                  comment='Foreign key to datasources table'),
        sa.Column('name', sa.String(255), nullable=False,
                  comment='Metric name (e.g., cpu.usage, memory.bytes)'),
        sa.Column('unit', sa.String(50), nullable=True,
                  comment='Unit of measurement (e.g., percent, bytes, ms)'),
        sa.Column('metric_type', sa.String(50), nullable=False,
                  comment='OTLP metric type: gauge, sum, histogram, summary'),
        sa.Column('description', sa.Text(), nullable=True,
                  comment='Metric description if provided in OTLP'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        comment='Metric definitions with types and units'
    )

    # Foreign key to datasources
    op.create_foreign_key(
        'fk_metric_definitions_datasource',
        'metric_definitions',
        'datasources',
        ['datasource_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # Unique constraint on datasource_id + name
    op.create_index(
        'ix_metric_definitions_datasource_name',
        'metric_definitions',
        ['datasource_id', 'name'],
        unique=True
    )

    # Index for name lookups
    op.create_index(
        'ix_metric_definitions_name',
        'metric_definitions',
        ['name']
    )

    # 4. METRIC_DATA TABLE
    # Time-series metric values with optimized indexes
    op.create_table(
        'metric_data',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('resource_id', sa.Integer(), nullable=False,
                  comment='Foreign key to resources table'),
        sa.Column('metric_definition_id', sa.Integer(), nullable=False,
                  comment='Foreign key to metric_definitions table'),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False,
                  comment='Metric timestamp from OTLP timeUnixNano'),
        sa.Column('value_double', sa.Float(), nullable=True,
                  comment='Metric value for gauge/sum with asDouble'),
        sa.Column('value_int', sa.BigInteger(), nullable=True,
                  comment='Metric value for gauge/sum with asInt'),
        sa.Column('attributes', postgresql.JSONB(), nullable=True,
                  comment='Additional datapoint attributes from OTLP'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        comment='Time-series metric data points'
    )

    # Foreign key to resources
    op.create_foreign_key(
        'fk_metric_data_resource',
        'metric_data',
        'resources',
        ['resource_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # Foreign key to metric_definitions
    op.create_foreign_key(
        'fk_metric_data_metric_definition',
        'metric_data',
        'metric_definitions',
        ['metric_definition_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # PRIMARY INDEX: Time-series queries ordered by timestamp DESC
    op.create_index(
        'ix_metric_data_timestamp_desc',
        'metric_data',
        [sa.text('timestamp DESC')]
    )

    # COMPOSITE INDEX: Query specific metric for a resource over time
    op.create_index(
        'ix_metric_data_resource_metric_time',
        'metric_data',
        ['resource_id', 'metric_definition_id', sa.text('timestamp DESC')]
    )

    # COMPOSITE INDEX: Aggregate metrics across resources
    op.create_index(
        'ix_metric_data_metric_time',
        'metric_data',
        ['metric_definition_id', sa.text('timestamp DESC')]
    )

    # INDEX: Resource-based queries
    op.create_index(
        'ix_metric_data_resource_time',
        'metric_data',
        ['resource_id', sa.text('timestamp DESC')]
    )

    # GIN index for JSONB attributes
    op.create_index(
        'ix_metric_data_attributes',
        'metric_data',
        ['attributes'],
        postgresql_using='gin'
    )

    # 5. PROCESSING_STATUS TABLE
    # Tracks which lm_metrics records have been processed
    op.create_table(
        'processing_status',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('lm_metrics_id', sa.Integer(), nullable=False,
                  comment='Foreign key to lm_metrics.id'),
        sa.Column('status', sa.String(50), nullable=False,
                  comment='Processing status: pending, processing, success, failed'),
        sa.Column('processed_at', sa.TIMESTAMP(timezone=True), nullable=True,
                  comment='When processing completed'),
        sa.Column('error_message', sa.Text(), nullable=True,
                  comment='Error message if status=failed'),
        sa.Column('metrics_extracted', sa.Integer(), nullable=True,
                  comment='Number of metric data points extracted'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('NOW()'), nullable=False),
        comment='Tracks processing status of lm_metrics records'
    )

    # Foreign key to lm_metrics
    op.create_foreign_key(
        'fk_processing_status_lm_metrics',
        'processing_status',
        'lm_metrics',
        ['lm_metrics_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # Unique constraint - each lm_metrics record processed once
    op.create_index(
        'ix_processing_status_lm_metrics_id',
        'processing_status',
        ['lm_metrics_id'],
        unique=True
    )

    # Index for status queries
    op.create_index(
        'ix_processing_status_status',
        'processing_status',
        ['status', 'created_at']
    )


def downgrade() -> None:
    """
    Drop normalized OTLP tables.

    Tables are dropped in reverse dependency order.
    """
    # Drop in reverse order to satisfy foreign key constraints
    op.drop_table('processing_status')
    op.drop_table('metric_data')
    op.drop_table('metric_definitions')
    op.drop_table('datasources')
    op.drop_table('resources')
