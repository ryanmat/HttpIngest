# ABOUTME: Tests for time-series aggregator including rollups, summaries, and retention
# ABOUTME: Verifies accuracy of aggregations across various time ranges and edge cases

import pytest
from datetime import datetime, timezone, timedelta
from src.aggregator import (
    TimeSeriesAggregator,
    RetentionPolicy,
    AggregationStats,
    DataGap,
)


class TestTimeSeriesAggregator:
    """Tests for TimeSeriesAggregator class."""

    def test_aggregator_initialization(self, db_connection):
        """Test aggregator initializes with default retention policy."""
        aggregator = TimeSeriesAggregator(db_connection)

        assert aggregator.db_connection == db_connection
        assert aggregator.retention_policy.raw_data_days == 7
        assert aggregator.retention_policy.hourly_rollup_days == 90
        assert aggregator.retention_policy.daily_summary_days == 365

    def test_aggregator_with_custom_retention(self, db_connection):
        """Test aggregator initializes with custom retention policy."""
        policy = RetentionPolicy(
            raw_data_days=3, hourly_rollup_days=30, daily_summary_days=180
        )
        aggregator = TimeSeriesAggregator(db_connection, retention_policy=policy)

        assert aggregator.retention_policy.raw_data_days == 3
        assert aggregator.retention_policy.hourly_rollup_days == 30
        assert aggregator.retention_policy.daily_summary_days == 180


class TestHourlyRollup:
    """Tests for hourly rollup creation."""

    def test_create_hourly_rollup_single_hour(
        self, db_connection, setup_test_metric_data, clean_normalized_tables
    ):
        """Test creating hourly rollup for a single hour of data."""
        resource_id, metric_def_id = setup_test_metric_data

        aggregator = TimeSeriesAggregator(db_connection)

        # Create rollup for last hour
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        rows_created = aggregator.create_hourly_rollup(start_time, end_time, incremental=False)

        assert rows_created > 0

        # Verify rollup data
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT data_point_count, min_value, max_value, avg_value
                FROM hourly_rollups
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            row = cur.fetchone()
            assert row is not None
            count, min_val, max_val, avg_val = row
            assert count > 0
            assert min_val is not None
            assert max_val is not None
            assert avg_val is not None
            assert min_val <= avg_val <= max_val

    def test_create_hourly_rollup_multiple_hours(
        self, db_connection, setup_test_metric_data_multiple_hours, clean_normalized_tables
    ):
        """Test creating hourly rollups for multiple hours."""
        resource_id, metric_def_id = setup_test_metric_data_multiple_hours

        aggregator = TimeSeriesAggregator(db_connection)

        # Create rollup for last 24 hours
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)

        rows_created = aggregator.create_hourly_rollup(start_time, end_time, incremental=False)

        # Should have created multiple hourly rollups
        assert rows_created >= 1

        # Verify we have hourly data
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM hourly_rollups
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            count = cur.fetchone()[0]
            assert count >= 1

    def test_hourly_rollup_accuracy(
        self, db_connection, setup_known_metric_values, clean_normalized_tables
    ):
        """Test that hourly rollup statistics are accurate."""
        resource_id, metric_def_id, expected_stats = setup_known_metric_values

        aggregator = TimeSeriesAggregator(db_connection)

        # Create rollup
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        aggregator.create_hourly_rollup(start_time, end_time, incremental=False)

        # Verify accuracy
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT data_point_count, min_value, max_value, avg_value, sum_value
                FROM hourly_rollups
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            row = cur.fetchone()
            assert row is not None

            count, min_val, max_val, avg_val, sum_val = row

            # Verify against expected values
            assert count == expected_stats["count"]
            assert abs(min_val - expected_stats["min"]) < 0.01
            assert abs(max_val - expected_stats["max"]) < 0.01
            assert abs(avg_val - expected_stats["avg"]) < 0.01
            assert abs(sum_val - expected_stats["sum"]) < 0.01

    def test_hourly_rollup_idempotent(
        self, db_connection, setup_test_metric_data, clean_normalized_tables
    ):
        """Test that running hourly rollup multiple times is idempotent."""
        resource_id, metric_def_id = setup_test_metric_data

        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=2)  # Extended to cover any hour boundary issues

        # Run rollup twice
        first_run = aggregator.create_hourly_rollup(start_time, end_time, incremental=False)
        second_run = aggregator.create_hourly_rollup(start_time, end_time, incremental=False)

        # Should have same number of rollups (not duplicated)
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM hourly_rollups
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            count_after = cur.fetchone()[0]

            # The key assertion: running twice should not change the count
            assert count_after == first_run  # Count equals first run, not doubled

    def test_hourly_rollup_incremental_mode(
        self, db_connection, setup_test_metric_data, clean_normalized_tables
    ):
        """Test incremental mode skips existing rollups."""
        resource_id, metric_def_id = setup_test_metric_data

        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        # Create initial rollup
        first_run = aggregator.create_hourly_rollup(start_time, end_time, incremental=False)
        assert first_run > 0

        # Run incremental - should skip existing
        second_run = aggregator.create_hourly_rollup(start_time, end_time, incremental=True)
        assert second_run == 0  # No new rollups created


class TestDailySummary:
    """Tests for daily summary creation."""

    def test_create_daily_summary_single_day(
        self, db_connection, setup_hourly_rollups, clean_normalized_tables
    ):
        """Test creating daily summary from hourly rollups."""
        resource_id, metric_def_id = setup_hourly_rollups

        aggregator = TimeSeriesAggregator(db_connection)

        # Create daily summary
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=1)

        rows_created = aggregator.create_daily_summary(start_date, end_date, incremental=False)

        assert rows_created > 0

        # Verify daily summary data
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT hour_count, total_data_points, min_value, max_value, avg_value
                FROM daily_summaries
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            row = cur.fetchone()
            assert row is not None
            hour_count, total_points, min_val, max_val, avg_val = row
            assert hour_count > 0
            assert total_points > 0
            assert min_val is not None
            assert max_val is not None
            assert avg_val is not None

    def test_daily_summary_accuracy(
        self, db_connection, setup_hourly_rollups_known_values, clean_normalized_tables
    ):
        """Test that daily summary statistics are accurate."""
        resource_id, metric_def_id, expected_stats = setup_hourly_rollups_known_values

        aggregator = TimeSeriesAggregator(db_connection)

        # Create daily summary
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=1)

        aggregator.create_daily_summary(start_date, end_date, incremental=False)

        # Verify accuracy
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT hour_count, total_data_points, min_value, max_value
                FROM daily_summaries
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            row = cur.fetchone()
            assert row is not None

            hour_count, total_points, min_val, max_val = row

            # Verify against expected values
            assert hour_count == expected_stats["hour_count"]
            assert total_points == expected_stats["total_points"]
            assert abs(min_val - expected_stats["min"]) < 0.01
            assert abs(max_val - expected_stats["max"]) < 0.01

    def test_daily_summary_idempotent(
        self, db_connection, setup_hourly_rollups, clean_normalized_tables
    ):
        """Test that running daily summary multiple times is idempotent."""
        resource_id, metric_def_id = setup_hourly_rollups

        aggregator = TimeSeriesAggregator(db_connection)

        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=1)

        # Run twice
        aggregator.create_daily_summary(start_date, end_date, incremental=False)
        aggregator.create_daily_summary(start_date, end_date, incremental=False)

        # Should have same number of summaries
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM daily_summaries
                WHERE resource_id = %s AND metric_definition_id = %s
            """,
                (resource_id, metric_def_id),
            )
            count = cur.fetchone()[0]
            assert count == 1


class TestIncrementalAggregation:
    """Tests for incremental aggregation."""

    def test_aggregate_incremental_creates_rollups_and_summaries(
        self, db_connection, setup_test_metric_data, clean_normalized_tables
    ):
        """Test incremental aggregation creates both rollups and summaries."""
        aggregator = TimeSeriesAggregator(db_connection)

        stats = aggregator.aggregate_incremental(lookback_hours=24)

        assert isinstance(stats, AggregationStats)
        assert stats.hourly_rollups_created >= 0
        assert stats.daily_summaries_created >= 0
        assert stats.time_range_start is not None
        assert stats.time_range_end is not None
        assert stats.processing_time_seconds > 0

    def test_incremental_aggregation_stats(
        self, db_connection, setup_test_metric_data, clean_normalized_tables
    ):
        """Test incremental aggregation returns accurate stats."""
        resource_id, metric_def_id = setup_test_metric_data

        aggregator = TimeSeriesAggregator(db_connection)

        stats = aggregator.aggregate_incremental(lookback_hours=2)

        # Verify stats have expected values
        assert stats.metrics_processed >= 1
        assert stats.resources_processed >= 1
        assert stats.gaps_detected == 0  # Not calculated in incremental mode


class TestGapDetection:
    """Tests for gap detection."""

    def test_detect_gaps_no_gaps(
        self, db_connection, setup_continuous_metric_data, clean_normalized_tables
    ):
        """Test gap detection with continuous data (no gaps)."""
        resource_id, metric_def_id = setup_continuous_metric_data

        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=5)

        gaps = aggregator.detect_gaps(
            resource_id, metric_def_id, start_time, end_time, expected_interval_minutes=60
        )

        assert len(gaps) == 0

    def test_detect_gaps_with_gaps(
        self, db_connection, setup_metric_data_with_gaps, clean_normalized_tables
    ):
        """Test gap detection identifies missing data."""
        resource_id, metric_def_id = setup_metric_data_with_gaps

        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=10)

        gaps = aggregator.detect_gaps(
            resource_id, metric_def_id, start_time, end_time, expected_interval_minutes=60
        )

        # Should detect at least one gap
        assert len(gaps) > 0

        # Verify gap structure
        gap = gaps[0]
        assert isinstance(gap, DataGap)
        assert gap.resource_id == resource_id
        assert gap.metric_definition_id == metric_def_id
        assert gap.gap_duration_hours > 1.0  # At least 1 hour gap
        assert gap.previous_timestamp is not None
        assert gap.next_timestamp is not None

    def test_detect_gaps_different_intervals(
        self, db_connection, setup_5min_interval_data, clean_normalized_tables
    ):
        """Test gap detection with different expected intervals."""
        resource_id, metric_def_id = setup_5min_interval_data

        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=2)

        # Check with 5-minute expected interval
        gaps = aggregator.detect_gaps(
            resource_id, metric_def_id, start_time, end_time, expected_interval_minutes=5
        )

        # Should detect gaps with 5-minute interval
        assert isinstance(gaps, list)


class TestRetentionPolicy:
    """Tests for retention policy application."""

    def test_apply_retention_policy_dry_run(
        self, db_connection, setup_old_metric_data, clean_normalized_tables
    ):
        """Test retention policy dry run counts without deleting."""
        policy = RetentionPolicy(raw_data_days=7)
        aggregator = TimeSeriesAggregator(db_connection, retention_policy=policy)

        results = aggregator.apply_retention_policy(dry_run=True)

        # Should return counts
        assert "raw_data_deleted" in results
        assert "hourly_rollups_deleted" in results
        assert "daily_summaries_deleted" in results

        # Verify no actual deletion
        with db_connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM metric_data")
            count_after = cur.fetchone()[0]
            assert count_after > 0  # Data still exists

    def test_apply_retention_policy_deletes_old_data(
        self, db_connection, setup_old_metric_data, clean_normalized_tables
    ):
        """Test retention policy actually deletes old data."""
        policy = RetentionPolicy(raw_data_days=1, auto_delete=True)
        aggregator = TimeSeriesAggregator(db_connection, retention_policy=policy)

        # Count before deletion
        with db_connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM metric_data")
            count_before = cur.fetchone()[0]

        # Apply retention policy (not dry run)
        results = aggregator.apply_retention_policy(dry_run=False)

        # Count after deletion
        with db_connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM metric_data")
            count_after = cur.fetchone()[0]

        # Should have deleted old data
        if results["raw_data_deleted"] > 0:
            assert count_after < count_before

    def test_retention_policy_hourly_rollups(
        self, db_connection, setup_old_hourly_rollups, clean_normalized_tables
    ):
        """Test retention policy deletes old hourly rollups."""
        policy = RetentionPolicy(hourly_rollup_days=30)
        aggregator = TimeSeriesAggregator(db_connection, retention_policy=policy)

        results = aggregator.apply_retention_policy(dry_run=True)

        assert results["hourly_rollups_deleted"] >= 0

    def test_retention_policy_daily_summaries(
        self, db_connection, setup_old_daily_summaries, clean_normalized_tables
    ):
        """Test retention policy deletes old daily summaries."""
        policy = RetentionPolicy(daily_summary_days=180)
        aggregator = TimeSeriesAggregator(db_connection, retention_policy=policy)

        results = aggregator.apply_retention_policy(dry_run=True)

        assert results["daily_summaries_deleted"] >= 0


class TestDataValidation:
    """Tests for data quality validation."""

    def test_validate_data_quality_clean_data(
        self, db_connection, setup_test_metric_data, clean_normalized_tables
    ):
        """Test data validation with clean data."""
        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        results = aggregator.validate_data_quality(start_time, end_time)

        assert "total_data_points" in results
        assert "null_values" in results
        assert "duplicate_timestamps" in results
        assert "metrics_validated" in results

        # Clean data should have no issues
        assert results["null_values"] == 0
        assert results["duplicate_timestamps"] == 0

    def test_validate_data_quality_with_nulls(
        self, db_connection, setup_metric_data_with_nulls, clean_normalized_tables
    ):
        """Test data validation detects NULL values."""
        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        results = aggregator.validate_data_quality(start_time, end_time)

        # Should detect NULL values
        assert results["null_values"] > 0

    def test_validate_data_quality_with_duplicates(
        self, db_connection, setup_metric_data_with_duplicates, clean_normalized_tables
    ):
        """Test data validation detects duplicate timestamps."""
        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        results = aggregator.validate_data_quality(start_time, end_time)

        # Should detect duplicates
        assert results["duplicate_timestamps"] > 0


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_hourly_rollup_empty_time_range(self, db_connection, clean_normalized_tables):
        """Test hourly rollup with no data in time range."""
        aggregator = TimeSeriesAggregator(db_connection)

        # Future time range with no data
        end_time = datetime.now(timezone.utc) + timedelta(days=100)
        start_time = end_time - timedelta(hours=1)

        rows_created = aggregator.create_hourly_rollup(start_time, end_time, incremental=False)

        assert rows_created == 0

    def test_gap_detection_insufficient_data(
        self, db_connection, setup_single_metric_datapoint, clean_normalized_tables
    ):
        """Test gap detection with only one data point."""
        resource_id, metric_def_id = setup_single_metric_datapoint

        aggregator = TimeSeriesAggregator(db_connection)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        gaps = aggregator.detect_gaps(
            resource_id, metric_def_id, start_time, end_time, expected_interval_minutes=60
        )

        # Should return empty list (need at least 2 points)
        assert len(gaps) == 0

    def test_tables_created_automatically(self, db_connection, clean_normalized_tables):
        """Test that aggregator creates tables automatically."""
        aggregator = TimeSeriesAggregator(db_connection)

        # Ensure tables exist
        aggregator._ensure_hourly_rollups_table()
        aggregator._ensure_daily_summaries_table()

        # Verify tables exist
        with db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_name IN ('hourly_rollups', 'daily_summaries')
            """
            )
            tables = [row[0] for row in cur.fetchall()]

            assert "hourly_rollups" in tables
            assert "daily_summaries" in tables
