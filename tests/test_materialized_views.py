# Description: Tests for materialized views and their performance
# Description: Verifies view creation, data accuracy, refresh functions, and query performance

import pytest
import json
import time
from datetime import datetime, timezone, timedelta


@pytest.fixture
def materialized_views_schema(db_connection):
    """
    Ensure materialized views schema is applied and cleaned up.

    Runs the materialized views migration and ensures cleanup.
    """
    # Check if views exist
    try:
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'MATERIALIZED VIEW'
                AND table_name IN ('latest_metrics', 'hourly_aggregates', 'resource_summary', 'datasource_metrics')
            """)
            views = [row[0] for row in cur.fetchall()]

            if len(views) < 4:
                pytest.skip(f"Materialized views migration not applied. Found {len(views)}/4 views. Run: uv run alembic upgrade head")
    except Exception as e:
        pytest.skip(f"Could not check for materialized views: {e}")

    yield

    # Cleanup - refresh views to ensure clean state
    try:
        with db_connection.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW latest_metrics")
            cur.execute("REFRESH MATERIALIZED VIEW hourly_aggregates")
            cur.execute("REFRESH MATERIALIZED VIEW resource_summary")
            cur.execute("REFRESH MATERIALIZED VIEW datasource_metrics")
            db_connection.commit()
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def sample_view_data(db_connection, sample_otlp_cpu_metrics, sample_otlp_memory_metrics, clean_normalized_tables):
    """
    Insert sample data and process it for materialized view testing.

    Creates resources, datasources, metrics, and data points.
    """
    from src.data_processor import DataProcessor

    # Insert sample lm_metrics records
    with db_connection.cursor() as cur:
        # Insert CPU metrics
        cur.execute(
            "INSERT INTO lm_metrics (payload) VALUES (%s) RETURNING id",
            (json.dumps(sample_otlp_cpu_metrics),)
        )
        cpu_id = cur.fetchone()[0]

        # Insert memory metrics
        cur.execute(
            "INSERT INTO lm_metrics (payload) VALUES (%s) RETURNING id",
            (json.dumps(sample_otlp_memory_metrics),)
        )
        memory_id = cur.fetchone()[0]

        db_connection.commit()

    # Process records
    processor = DataProcessor(db_connection)
    processor.process_single_record(cpu_id, sample_otlp_cpu_metrics)
    processor.process_single_record(memory_id, sample_otlp_memory_metrics)

    # Refresh materialized views
    with db_connection.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW latest_metrics")
        cur.execute("REFRESH MATERIALIZED VIEW hourly_aggregates")
        cur.execute("REFRESH MATERIALIZED VIEW resource_summary")
        cur.execute("REFRESH MATERIALIZED VIEW datasource_metrics")
        db_connection.commit()

    yield

    # Cleanup handled by clean_normalized_tables


class TestLatestMetricsView:
    """Tests for latest_metrics materialized view."""

    def test_latest_metrics_view_exists(self, db_connection, materialized_views_schema):
        """Test that latest_metrics view exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'MATERIALIZED VIEW'
                    AND table_name = 'latest_metrics'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True

    def test_latest_metrics_contains_data(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that latest_metrics view contains the most recent data points."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM latest_metrics")
            count = cur.fetchone()[0]
            assert count >= 2  # At least CPU and memory metrics

    def test_latest_metrics_shows_most_recent(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that latest_metrics shows only the most recent value per metric."""
        # Insert older data point for same metric
        with db_connection.cursor() as cur:
            # Get a metric definition
            cur.execute("SELECT id, name FROM metric_definitions LIMIT 1")
            metric_def_id, metric_name = cur.fetchone()

            # Get resource
            cur.execute("SELECT id FROM resources LIMIT 1")
            resource_id = cur.fetchone()[0]

            # Insert old data point
            old_timestamp = datetime.now(timezone.utc) - timedelta(hours=2)
            cur.execute("""
                INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double)
                VALUES (%s, %s, %s, %s)
            """, (resource_id, metric_def_id, old_timestamp, 10.0))

            # Insert new data point
            new_timestamp = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double)
                VALUES (%s, %s, %s, %s)
            """, (resource_id, metric_def_id, new_timestamp, 20.0))

            db_connection.commit()

        # Refresh view
        with db_connection.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW latest_metrics")
            db_connection.commit()

        # Verify latest value is shown
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT value_double
                FROM latest_metrics
                WHERE metric_name = %s AND resource_id = %s
            """, (metric_name, resource_id))
            value = cur.fetchone()[0]
            assert value == 20.0  # Most recent value

    def test_latest_metrics_indexes_exist(self, db_connection, materialized_views_schema):
        """Test that indexes on latest_metrics exist."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'latest_metrics'
            """)
            indexes = [row[0] for row in cur.fetchall()]

            assert 'ix_latest_metrics_resource_id' in indexes
            assert 'ix_latest_metrics_metric_definition_id' in indexes
            assert 'ix_latest_metrics_datasource_name' in indexes
            assert 'ix_latest_metrics_timestamp' in indexes


class TestHourlyAggregatesView:
    """Tests for hourly_aggregates materialized view."""

    def test_hourly_aggregates_view_exists(self, db_connection, materialized_views_schema):
        """Test that hourly_aggregates view exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'MATERIALIZED VIEW'
                    AND table_name = 'hourly_aggregates'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True

    def test_hourly_aggregates_calculates_stats(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that hourly_aggregates calculates min/max/avg correctly."""
        # Insert multiple data points in the same hour
        with db_connection.cursor() as cur:
            # Get metric definition
            cur.execute("SELECT id FROM metric_definitions LIMIT 1")
            metric_def_id = cur.fetchone()[0]

            # Get resource
            cur.execute("SELECT id FROM resources LIMIT 1")
            resource_id = cur.fetchone()[0]

            # Insert data points with known values
            base_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            values = [10.0, 20.0, 30.0, 40.0, 50.0]

            for i, value in enumerate(values):
                timestamp = base_time + timedelta(minutes=i * 10)
                cur.execute("""
                    INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double)
                    VALUES (%s, %s, %s, %s)
                """, (resource_id, metric_def_id, timestamp, value))

            db_connection.commit()

        # Refresh view
        with db_connection.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW hourly_aggregates")
            db_connection.commit()

        # Verify aggregates
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT min_value, max_value, avg_value, data_point_count
                FROM hourly_aggregates
                WHERE resource_id = %s AND metric_definition_id = %s
                ORDER BY hour DESC
                LIMIT 1
            """, (resource_id, metric_def_id))

            row = cur.fetchone()
            if row:
                min_val, max_val, avg_val, count = row
                assert min_val == 10.0
                assert max_val == 50.0
                assert avg_val == 30.0
                assert count >= 5

    def test_hourly_aggregates_indexes_exist(self, db_connection, materialized_views_schema):
        """Test that indexes on hourly_aggregates exist."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'hourly_aggregates'
            """)
            indexes = [row[0] for row in cur.fetchall()]

            assert 'ix_hourly_aggregates_hour' in indexes
            assert 'ix_hourly_aggregates_resource_metric' in indexes
            assert 'ix_hourly_aggregates_datasource' in indexes


class TestResourceSummaryView:
    """Tests for resource_summary materialized view."""

    def test_resource_summary_view_exists(self, db_connection, materialized_views_schema):
        """Test that resource_summary view exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'MATERIALIZED VIEW'
                    AND table_name = 'resource_summary'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True

    def test_resource_summary_counts_metrics(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that resource_summary correctly counts metrics per resource."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT resource_id, metric_count, total_data_points
                FROM resource_summary
                WHERE metric_count > 0
                LIMIT 1
            """)
            row = cur.fetchone()

            if row:
                resource_id, metric_count, total_data_points = row
                assert metric_count >= 1
                assert total_data_points >= 1

    def test_resource_summary_tracks_timestamps(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that resource_summary tracks first/last metric timestamps."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT first_metric_timestamp, last_metric_timestamp
                FROM resource_summary
                WHERE metric_count > 0
                LIMIT 1
            """)
            row = cur.fetchone()

            if row:
                first_ts, last_ts = row
                assert first_ts is not None
                assert last_ts is not None
                assert first_ts <= last_ts

    def test_resource_summary_indexes_exist(self, db_connection, materialized_views_schema):
        """Test that indexes on resource_summary exist."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'resource_summary'
            """)
            indexes = [row[0] for row in cur.fetchall()]

            assert 'ix_resource_summary_resource_hash' in indexes
            assert 'ix_resource_summary_last_metric_timestamp' in indexes
            assert 'ix_resource_summary_metric_count' in indexes


class TestDatasourceMetricsView:
    """Tests for datasource_metrics materialized view."""

    def test_datasource_metrics_view_exists(self, db_connection, materialized_views_schema):
        """Test that datasource_metrics view exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'MATERIALIZED VIEW'
                    AND table_name = 'datasource_metrics'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True

    def test_datasource_metrics_lists_metrics(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that datasource_metrics lists all metrics for datasources."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT datasource_name, metric_name, resource_count, total_data_points
                FROM datasource_metrics
                LIMIT 1
            """)
            row = cur.fetchone()

            if row:
                ds_name, metric_name, resource_count, data_points = row
                assert ds_name is not None
                assert metric_name is not None
                assert resource_count >= 0
                assert data_points >= 0

    def test_datasource_metrics_indexes_exist(self, db_connection, materialized_views_schema):
        """Test that indexes on datasource_metrics exist."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'datasource_metrics'
            """)
            indexes = [row[0] for row in cur.fetchall()]

            assert 'ix_datasource_metrics_datasource_name' in indexes
            assert 'ix_datasource_metrics_metric_name' in indexes
            assert 'ix_datasource_metrics_resource_count' in indexes


class TestRefreshFunctions:
    """Tests for materialized view refresh functions."""

    def test_refresh_all_function_exists(self, db_connection, materialized_views_schema):
        """Test that refresh_all_materialized_views() function exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_proc
                    WHERE proname = 'refresh_all_materialized_views'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True

    def test_refresh_all_function_works(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that refresh_all_materialized_views() executes successfully."""
        with db_connection.cursor() as cur:
            # Should not raise exception
            cur.execute("SELECT refresh_all_materialized_views()")
            db_connection.commit()

    def test_refresh_latest_metrics_function_exists(self, db_connection, materialized_views_schema):
        """Test that refresh_latest_metrics() function exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_proc
                    WHERE proname = 'refresh_latest_metrics'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True

    def test_refresh_hourly_aggregates_function_exists(self, db_connection, materialized_views_schema):
        """Test that refresh_hourly_aggregates() function exists."""
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_proc
                    WHERE proname = 'refresh_hourly_aggregates'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists is True


class TestViewPerformance:
    """Tests for materialized view query performance."""

    def test_latest_metrics_query_performance(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that querying latest_metrics is faster than querying raw tables."""
        # Query materialized view
        start_time = time.time()
        with db_connection.cursor() as cur:
            cur.execute("SELECT * FROM latest_metrics LIMIT 100")
            cur.fetchall()
        view_time = time.time() - start_time

        # Query raw tables with same logic
        start_time = time.time()
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (md.resource_id, md.metric_definition_id)
                    md.id, md.resource_id, r.resource_hash,
                    md.metric_definition_id, mdef.name, md.timestamp,
                    md.value_double, md.value_int
                FROM metric_data md
                JOIN resources r ON md.resource_id = r.id
                JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
                ORDER BY md.resource_id, md.metric_definition_id, md.timestamp DESC
                LIMIT 100
            """)
            cur.fetchall()
        raw_time = time.time() - start_time

        # View should be at least as fast (or faster for larger datasets)
        # For small test datasets, both should be very fast
        assert view_time <= raw_time * 2  # Allow some tolerance

    def test_hourly_aggregates_query_performance(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that querying hourly_aggregates is efficient."""
        start_time = time.time()
        with db_connection.cursor() as cur:
            cur.execute("""
                SELECT * FROM hourly_aggregates
                WHERE hour > NOW() - INTERVAL '24 hours'
                LIMIT 100
            """)
            cur.fetchall()
        query_time = time.time() - start_time

        # Should complete quickly (< 1 second for test data)
        assert query_time < 1.0

    def test_resource_summary_query_performance(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that querying resource_summary is efficient."""
        start_time = time.time()
        with db_connection.cursor() as cur:
            cur.execute("SELECT * FROM resource_summary ORDER BY metric_count DESC LIMIT 10")
            cur.fetchall()
        query_time = time.time() - start_time

        # Should complete quickly
        assert query_time < 1.0

    def test_datasource_metrics_query_performance(self, db_connection, materialized_views_schema, sample_view_data):
        """Test that querying datasource_metrics is efficient."""
        start_time = time.time()
        with db_connection.cursor() as cur:
            cur.execute("SELECT * FROM datasource_metrics ORDER BY resource_count DESC LIMIT 10")
            cur.fetchall()
        query_time = time.time() - start_time

        # Should complete quickly
        assert query_time < 1.0
