# Description: Tests for normalized OTLP schema created by migration
# Description: Validates table structure, foreign keys, indexes, and data storage

import pytest
import json
import hashlib
from datetime import datetime, timezone
from alembic.config import Config
from alembic import command
from pathlib import Path


@pytest.fixture(scope="module")
def alembic_config():
    """Alembic configuration for migrations."""
    project_root = Path(__file__).parent.parent
    config_path = project_root / "alembic.ini"
    return Config(str(config_path))


@pytest.fixture(scope="function")
def normalized_schema(db_connection, alembic_config):
    """
    Set up normalized schema by running migrations.

    Cleans up after test by rolling back migration.
    """
    # Drop alembic_version and all normalized tables if they exist
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS alembic_version CASCADE")
        cur.execute("DROP TABLE IF EXISTS processing_status CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_data CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_definitions CASCADE")
        cur.execute("DROP TABLE IF EXISTS datasources CASCADE")
        cur.execute("DROP TABLE IF EXISTS resources CASCADE")
        db_connection.commit()

    # Run migrations
    command.upgrade(alembic_config, "head")

    yield

    # Cleanup: Downgrade to remove normalized tables
    command.downgrade(alembic_config, "base")

    # Drop alembic_version
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS alembic_version CASCADE")
        db_connection.commit()


def test_resources_table_created(db_connection, normalized_schema):
    """Verify resources table exists with correct structure."""
    with db_connection.cursor() as cur:
        # Check table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'resources'
            )
        """)
        assert cur.fetchone()[0] is True

        # Check columns
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'resources'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: {'type': row[1], 'nullable': row[2]} for row in cur.fetchall()}

        assert 'id' in columns
        assert 'resource_hash' in columns
        assert 'attributes' in columns
        assert columns['attributes']['type'] == 'jsonb'
        assert 'created_at' in columns
        assert 'updated_at' in columns


def test_datasources_table_created(db_connection, normalized_schema):
    """Verify datasources table exists with correct structure."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'datasources'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cur.fetchall()}

        assert 'id' in columns
        assert 'name' in columns
        assert 'version' in columns
        assert 'created_at' in columns


def test_metric_definitions_table_created(db_connection, normalized_schema):
    """Verify metric_definitions table exists with correct structure."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'metric_definitions'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cur.fetchall()}

        assert 'id' in columns
        assert 'datasource_id' in columns
        assert 'name' in columns
        assert 'unit' in columns
        assert 'metric_type' in columns
        assert 'description' in columns


def test_metric_data_table_created(db_connection, normalized_schema):
    """Verify metric_data table exists with correct structure."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'metric_data'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cur.fetchall()}

        assert 'id' in columns
        assert 'resource_id' in columns
        assert 'metric_definition_id' in columns
        assert 'timestamp' in columns
        assert 'value_double' in columns
        assert 'value_int' in columns
        assert 'attributes' in columns
        assert columns['attributes'] == 'jsonb'


def test_processing_status_table_created(db_connection, normalized_schema):
    """Verify processing_status table exists with correct structure."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'processing_status'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cur.fetchall()}

        assert 'id' in columns
        assert 'lm_metrics_id' in columns
        assert 'status' in columns
        assert 'processed_at' in columns
        assert 'error_message' in columns
        assert 'metrics_extracted' in columns


def test_foreign_keys_exist(db_connection, normalized_schema):
    """Verify all foreign key constraints are created."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name IN (
                    'metric_definitions',
                    'metric_data',
                    'processing_status'
                )
        """)
        foreign_keys = cur.fetchall()

        # Should have 4 foreign keys total
        assert len(foreign_keys) == 4

        # Check specific foreign keys
        fk_dict = {(row[0], row[1]): (row[2], row[3]) for row in foreign_keys}

        assert ('metric_definitions', 'datasource_id') in fk_dict
        assert fk_dict[('metric_definitions', 'datasource_id')] == ('datasources', 'id')

        assert ('metric_data', 'resource_id') in fk_dict
        assert fk_dict[('metric_data', 'resource_id')] == ('resources', 'id')

        assert ('metric_data', 'metric_definition_id') in fk_dict
        assert fk_dict[('metric_data', 'metric_definition_id')] == ('metric_definitions', 'id')

        assert ('processing_status', 'lm_metrics_id') in fk_dict
        assert fk_dict[('processing_status', 'lm_metrics_id')] == ('lm_metrics', 'id')


def test_indexes_created(db_connection, normalized_schema):
    """Verify all indexes are created."""
    with db_connection.cursor() as cur:
        # Get all indexes for our tables
        cur.execute("""
            SELECT
                tablename,
                indexname
            FROM pg_indexes
            WHERE tablename IN (
                'resources',
                'datasources',
                'metric_definitions',
                'metric_data',
                'processing_status'
            )
            AND schemaname = 'public'
        """)
        indexes = {row[1]: row[0] for row in cur.fetchall()}

        # Check key indexes exist
        assert 'ix_resources_attributes' in indexes
        assert 'ix_datasources_name_version' in indexes
        assert 'ix_metric_definitions_datasource_name' in indexes
        assert 'ix_metric_definitions_name' in indexes
        assert 'ix_metric_data_timestamp_desc' in indexes
        assert 'ix_metric_data_resource_metric_time' in indexes
        assert 'ix_metric_data_metric_time' in indexes
        assert 'ix_processing_status_lm_metrics_id' in indexes
        assert 'ix_processing_status_status' in indexes


def test_store_otlp_resource(db_connection, normalized_schema):
    """Test storing a resource from OTLP data."""
    attributes = {
        "service.name": "web-server",
        "host.name": "server01",
        "environment": "production"
    }

    # Create resource hash
    attr_str = json.dumps(attributes, sort_keys=True)
    resource_hash = hashlib.sha256(attr_str.encode()).hexdigest()

    with db_connection.cursor() as cur:
        cur.execute("""
            INSERT INTO resources (resource_hash, attributes)
            VALUES (%s, %s)
            RETURNING id
        """, (resource_hash, json.dumps(attributes)))

        resource_id = cur.fetchone()[0]
        db_connection.commit()

        # Verify resource was stored
        cur.execute("SELECT attributes FROM resources WHERE id = %s", (resource_id,))
        stored_attrs = cur.fetchone()[0]

        assert stored_attrs['service.name'] == 'web-server'
        assert stored_attrs['host.name'] == 'server01'


def test_store_complete_otlp_metric(db_connection, normalized_schema, sample_otlp_cpu_metrics):
    """Test storing a complete OTLP metric with all normalized tables."""
    # Parse OTLP data
    otlp_data = sample_otlp_cpu_metrics
    resource_metrics = otlp_data['resourceMetrics'][0]
    resource_attrs = {attr['key']: attr['value'].get('stringValue', attr['value'])
                      for attr in resource_metrics['resource']['attributes']}
    scope = resource_metrics['scopeMetrics'][0]['scope']
    metric = resource_metrics['scopeMetrics'][0]['metrics'][0]
    data_point = metric['gauge']['dataPoints'][0]

    with db_connection.cursor() as cur:
        # 1. Insert resource
        attr_str = json.dumps(resource_attrs, sort_keys=True)
        resource_hash = hashlib.sha256(attr_str.encode()).hexdigest()

        cur.execute("""
            INSERT INTO resources (resource_hash, attributes)
            VALUES (%s, %s)
            ON CONFLICT (resource_hash) DO UPDATE SET updated_at = NOW()
            RETURNING id
        """, (resource_hash, json.dumps(resource_attrs)))
        resource_id = cur.fetchone()[0]

        # 2. Insert datasource
        cur.execute("""
            INSERT INTO datasources (name, version)
            VALUES (%s, %s)
            ON CONFLICT (name, version) DO NOTHING
            RETURNING id
        """, (scope['name'], scope.get('version')))
        result = cur.fetchone()
        if result:
            datasource_id = result[0]
        else:
            cur.execute("""
                SELECT id FROM datasources WHERE name = %s AND version = %s
            """, (scope['name'], scope.get('version')))
            datasource_id = cur.fetchone()[0]

        # 3. Insert metric definition
        metric_type = 'gauge'  # from the gauge field in OTLP
        cur.execute("""
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (datasource_id, name) DO NOTHING
            RETURNING id
        """, (datasource_id, metric['name'], metric.get('unit'), metric_type))
        result = cur.fetchone()
        if result:
            metric_def_id = result[0]
        else:
            cur.execute("""
                SELECT id FROM metric_definitions
                WHERE datasource_id = %s AND name = %s
            """, (datasource_id, metric['name']))
            metric_def_id = cur.fetchone()[0]

        # 4. Insert metric data
        timestamp = datetime.fromtimestamp(data_point['timeUnixNano'] / 1e9, tz=timezone.utc)
        value_double = data_point.get('asDouble')
        value_int = data_point.get('asInt')

        cur.execute("""
            INSERT INTO metric_data (
                resource_id, metric_definition_id, timestamp, value_double, value_int
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (resource_id, metric_def_id, timestamp, value_double, value_int))
        metric_data_id = cur.fetchone()[0]

        db_connection.commit()

        # Verify data was stored correctly
        cur.execute("""
            SELECT
                r.attributes->>'service.name' as service,
                r.attributes->>'host.name' as host,
                ds.name as datasource,
                md.name as metric_name,
                md.unit,
                m.timestamp,
                m.value_double
            FROM metric_data m
            JOIN resources r ON m.resource_id = r.id
            JOIN metric_definitions md ON m.metric_definition_id = md.id
            JOIN datasources ds ON md.datasource_id = ds.id
            WHERE m.id = %s
        """, (metric_data_id,))

        row = cur.fetchone()
        assert row[0] == 'web-server'  # service
        assert row[1] == 'server01'  # host
        assert row[2] == 'CPU_Usage'  # datasource
        assert row[3] == 'cpu.usage'  # metric_name
        assert row[4] == 'percent'  # unit
        assert row[6] == 45.2  # value_double


def test_time_series_query_performance(db_connection, normalized_schema):
    """Test that time-series queries use indexes efficiently."""
    # Insert test data
    with db_connection.cursor() as cur:
        # Create resource
        cur.execute("""
            INSERT INTO resources (resource_hash, attributes)
            VALUES ('test_hash_123', '{"host": "test"}')
            RETURNING id
        """)
        resource_id = cur.fetchone()[0]

        # Create datasource
        cur.execute("""
            INSERT INTO datasources (name, version)
            VALUES ('TestDS', '1.0')
            RETURNING id
        """)
        datasource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute("""
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type)
            VALUES (%s, 'test.metric', 'count', 'gauge')
            RETURNING id
        """, (datasource_id,))
        metric_def_id = cur.fetchone()[0]

        # Insert multiple data points
        for i in range(10):
            timestamp = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double)
                VALUES (%s, %s, %s, %s)
            """, (resource_id, metric_def_id, timestamp, float(i)))

        db_connection.commit()

        # Query with EXPLAIN to verify index usage
        cur.execute("""
            EXPLAIN (FORMAT JSON)
            SELECT timestamp, value_double
            FROM metric_data
            WHERE resource_id = %s
                AND metric_definition_id = %s
            ORDER BY timestamp DESC
            LIMIT 100
        """, (resource_id, metric_def_id))

        explain_result = cur.fetchone()[0]
        explain_str = json.dumps(explain_result)

        # Verify an index scan is used (not seq scan)
        assert 'Index' in explain_str or 'index' in explain_str.lower()


def test_processing_status_tracking(db_connection, normalized_schema):
    """Test processing_status table functionality."""
    # First create a test record in lm_metrics
    with db_connection.cursor() as cur:
        cur.execute("""
            INSERT INTO lm_metrics (payload)
            VALUES (%s)
            RETURNING id
        """, (json.dumps({"test": "data"}),))
        lm_metrics_id = cur.fetchone()[0]
        db_connection.commit()

        # Insert processing status
        cur.execute("""
            INSERT INTO processing_status (lm_metrics_id, status, metrics_extracted)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (lm_metrics_id, 'success', 5))
        status_id = cur.fetchone()[0]
        db_connection.commit()

        # Verify status was stored
        cur.execute("""
            SELECT status, metrics_extracted
            FROM processing_status
            WHERE lm_metrics_id = %s
        """, (lm_metrics_id,))
        row = cur.fetchone()

        assert row[0] == 'success'
        assert row[1] == 5

        # Test unique constraint - should fail on duplicate
        with pytest.raises(Exception):
            cur.execute("""
                INSERT INTO processing_status (lm_metrics_id, status)
                VALUES (%s, %s)
            """, (lm_metrics_id, 'failed'))
            db_connection.commit()

        db_connection.rollback()


def test_cascade_delete_resources(db_connection, normalized_schema):
    """Test that deleting a resource cascades to metric_data."""
    with db_connection.cursor() as cur:
        # Create test data
        cur.execute("""
            INSERT INTO resources (resource_hash, attributes)
            VALUES ('cascade_test', '{"test": "resource"}')
            RETURNING id
        """)
        resource_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO datasources (name, version)
            VALUES ('CascadeDS', '1.0')
            RETURNING id
        """)
        datasource_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type)
            VALUES (%s, 'cascade.metric', 'test', 'gauge')
            RETURNING id
        """, (datasource_id,))
        metric_def_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double)
            VALUES (%s, %s, NOW(), 1.0)
            RETURNING id
        """, (resource_id, metric_def_id))
        metric_data_id = cur.fetchone()[0]

        db_connection.commit()

        # Delete resource - should cascade to metric_data
        cur.execute("DELETE FROM resources WHERE id = %s", (resource_id,))
        db_connection.commit()

        # Verify metric_data was also deleted
        cur.execute("SELECT COUNT(*) FROM metric_data WHERE id = %s", (metric_data_id,))
        count = cur.fetchone()[0]
        assert count == 0, "Metric data should be deleted via CASCADE"
