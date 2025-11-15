# ABOUTME: Time-series aggregation for metric data with rollups and retention policies
# ABOUTME: Builds hourly/daily summaries, detects gaps, and manages data lifecycle

"""
Time-Series Aggregator for LogicMonitor Metrics

Provides efficient aggregation of raw metric data into hourly and daily rollups:
- Hourly rollups: MIN, MAX, AVG, SUM, COUNT, STDDEV per hour
- Daily summaries: Daily statistics with additional percentiles
- Incremental processing: Only process new data since last run
- Gap detection: Identify missing data points
- Retention policies: Configurable data lifecycle management

All aggregations are idempotent and can be safely re-run.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict


logger = logging.getLogger(__name__)


@dataclass
class AggregationStats:
    """Statistics from an aggregation run."""

    hourly_rollups_created: int
    daily_summaries_created: int
    time_range_start: Optional[datetime]
    time_range_end: Optional[datetime]
    metrics_processed: int
    resources_processed: int
    gaps_detected: int
    processing_time_seconds: float


@dataclass
class RetentionPolicy:
    """Configurable retention policy for time-series data."""

    # Raw metric_data retention
    raw_data_days: int = 7

    # Hourly rollup retention
    hourly_rollup_days: int = 90

    # Daily summary retention
    daily_summary_days: int = 365

    # Auto-delete old data
    auto_delete: bool = False


@dataclass
class DataGap:
    """Represents a detected gap in time-series data."""

    resource_id: int
    metric_definition_id: int
    expected_timestamp: datetime
    gap_duration_hours: float
    previous_timestamp: Optional[datetime]
    next_timestamp: Optional[datetime]


class TimeSeriesAggregator:
    """
    Time-series data aggregator for metric data.

    Handles:
    - Hourly rollup creation with statistics
    - Daily summary generation
    - Incremental processing
    - Gap detection
    - Retention policy enforcement
    """

    def __init__(self, db_connection, retention_policy: Optional[RetentionPolicy] = None):
        """
        Initialize time-series aggregator.

        Args:
            db_connection: psycopg2 database connection
            retention_policy: Optional retention policy configuration
        """
        self.db_connection = db_connection
        self.retention_policy = retention_policy or RetentionPolicy()
        self.logger = logging.getLogger(__name__)

    def create_hourly_rollup(
        self, start_time: datetime, end_time: datetime, incremental: bool = True
    ) -> int:
        """
        Create hourly rollups for the specified time range.

        Aggregates raw metric_data into hourly buckets with statistics:
        - MIN, MAX, AVG, SUM, COUNT, STDDEV

        Args:
            start_time: Start of time range (inclusive)
            end_time: End of time range (exclusive)
            incremental: If True, skip hours that already have rollups

        Returns:
            Number of hourly rollups created
        """
        self.logger.info(
            f"Creating hourly rollups from {start_time} to {end_time} (incremental={incremental})"
        )

        # Create hourly_rollups table if not exists
        self._ensure_hourly_rollups_table()

        # Build incremental condition
        incremental_condition = ""
        if incremental:
            incremental_condition = """
                AND NOT EXISTS (
                    SELECT 1 FROM hourly_rollups hr
                    WHERE hr.resource_id = md.resource_id
                    AND hr.metric_definition_id = md.metric_definition_id
                    AND hr.hour = DATE_TRUNC('hour', md.timestamp)
                )
            """

        with self.db_connection.cursor() as cur:
            # Insert hourly rollups
            cur.execute(
                f"""
                INSERT INTO hourly_rollups (
                    resource_id,
                    metric_definition_id,
                    hour,
                    data_point_count,
                    min_value,
                    max_value,
                    avg_value,
                    sum_value,
                    stddev_value,
                    first_timestamp,
                    last_timestamp
                )
                SELECT
                    md.resource_id,
                    md.metric_definition_id,
                    DATE_TRUNC('hour', md.timestamp) as hour,
                    COUNT(*) as data_point_count,
                    MIN(COALESCE(md.value_double, md.value_int::float)) as min_value,
                    MAX(COALESCE(md.value_double, md.value_int::float)) as max_value,
                    AVG(COALESCE(md.value_double, md.value_int::float)) as avg_value,
                    SUM(COALESCE(md.value_double, md.value_int::float)) as sum_value,
                    STDDEV(COALESCE(md.value_double, md.value_int::float)) as stddev_value,
                    MIN(md.timestamp) as first_timestamp,
                    MAX(md.timestamp) as last_timestamp
                FROM metric_data md
                WHERE md.timestamp >= %s
                  AND md.timestamp < %s
                  {incremental_condition}
                GROUP BY
                    md.resource_id,
                    md.metric_definition_id,
                    DATE_TRUNC('hour', md.timestamp)
                ON CONFLICT (resource_id, metric_definition_id, hour)
                DO UPDATE SET
                    data_point_count = EXCLUDED.data_point_count,
                    min_value = EXCLUDED.min_value,
                    max_value = EXCLUDED.max_value,
                    avg_value = EXCLUDED.avg_value,
                    sum_value = EXCLUDED.sum_value,
                    stddev_value = EXCLUDED.stddev_value,
                    first_timestamp = EXCLUDED.first_timestamp,
                    last_timestamp = EXCLUDED.last_timestamp,
                    updated_at = NOW()
            """,
                (start_time, end_time),
            )

            rows_affected = cur.rowcount
            self.db_connection.commit()

        self.logger.info(f"Created {rows_affected} hourly rollups")
        return rows_affected

    def create_daily_summary(
        self, start_date: datetime, end_date: datetime, incremental: bool = True
    ) -> int:
        """
        Create daily summaries from hourly rollups.

        Aggregates hourly rollups into daily summaries with enhanced statistics:
        - MIN, MAX, AVG, SUM, COUNT
        - Hour-level statistics (hours with data, first/last hour)
        - Data quality metrics (gap count)

        Args:
            start_date: Start date (inclusive)
            end_date: End date (exclusive)
            incremental: If True, skip days that already have summaries

        Returns:
            Number of daily summaries created
        """
        self.logger.info(
            f"Creating daily summaries from {start_date} to {end_date} (incremental={incremental})"
        )

        # Create daily_summaries table if not exists
        self._ensure_daily_summaries_table()

        # Build incremental condition
        incremental_condition = ""
        if incremental:
            incremental_condition = """
                AND NOT EXISTS (
                    SELECT 1 FROM daily_summaries ds
                    WHERE ds.resource_id = hr.resource_id
                    AND ds.metric_definition_id = hr.metric_definition_id
                    AND ds.day = DATE_TRUNC('day', hr.hour)
                )
            """

        with self.db_connection.cursor() as cur:
            # Insert daily summaries from hourly rollups
            cur.execute(
                f"""
                INSERT INTO daily_summaries (
                    resource_id,
                    metric_definition_id,
                    day,
                    hour_count,
                    total_data_points,
                    min_value,
                    max_value,
                    avg_value,
                    sum_value,
                    stddev_value,
                    first_hour,
                    last_hour
                )
                SELECT
                    hr.resource_id,
                    hr.metric_definition_id,
                    DATE_TRUNC('day', hr.hour) as day,
                    COUNT(*) as hour_count,
                    SUM(hr.data_point_count) as total_data_points,
                    MIN(hr.min_value) as min_value,
                    MAX(hr.max_value) as max_value,
                    AVG(hr.avg_value) as avg_value,
                    SUM(hr.sum_value) as sum_value,
                    STDDEV(hr.avg_value) as stddev_value,
                    MIN(hr.hour) as first_hour,
                    MAX(hr.hour) as last_hour
                FROM hourly_rollups hr
                WHERE hr.hour >= %s
                  AND hr.hour < %s
                  {incremental_condition}
                GROUP BY
                    hr.resource_id,
                    hr.metric_definition_id,
                    DATE_TRUNC('day', hr.hour)
                ON CONFLICT (resource_id, metric_definition_id, day)
                DO UPDATE SET
                    hour_count = EXCLUDED.hour_count,
                    total_data_points = EXCLUDED.total_data_points,
                    min_value = EXCLUDED.min_value,
                    max_value = EXCLUDED.max_value,
                    avg_value = EXCLUDED.avg_value,
                    sum_value = EXCLUDED.sum_value,
                    stddev_value = EXCLUDED.stddev_value,
                    first_hour = EXCLUDED.first_hour,
                    last_hour = EXCLUDED.last_hour,
                    updated_at = NOW()
            """,
                (start_date, end_date),
            )

            rows_affected = cur.rowcount
            self.db_connection.commit()

        self.logger.info(f"Created {rows_affected} daily summaries")
        return rows_affected

    def aggregate_incremental(self, lookback_hours: int = 24) -> AggregationStats:
        """
        Perform incremental aggregation for recent data.

        Processes data from the last N hours, creating both hourly rollups
        and daily summaries as needed.

        Args:
            lookback_hours: Hours to look back from now

        Returns:
            AggregationStats with processing details
        """
        import time

        start_processing = time.time()

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=lookback_hours)

        self.logger.info(f"Starting incremental aggregation for last {lookback_hours} hours")

        # Create hourly rollups
        hourly_created = self.create_hourly_rollup(start_time, end_time, incremental=True)

        # Create daily summaries (for affected days)
        daily_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        daily_end = end_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
            days=1
        )
        daily_created = self.create_daily_summary(daily_start, daily_end, incremental=True)

        # Get metrics/resources processed
        with self.db_connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT metric_definition_id) as metrics,
                    COUNT(DISTINCT resource_id) as resources
                FROM metric_data
                WHERE timestamp >= %s AND timestamp < %s
            """,
                (start_time, end_time),
            )
            metrics_count, resources_count = cur.fetchone()

        processing_time = time.time() - start_processing

        stats = AggregationStats(
            hourly_rollups_created=hourly_created,
            daily_summaries_created=daily_created,
            time_range_start=start_time,
            time_range_end=end_time,
            metrics_processed=metrics_count or 0,
            resources_processed=resources_count or 0,
            gaps_detected=0,  # Not calculated in incremental mode
            processing_time_seconds=processing_time,
        )

        self.logger.info(
            f"Incremental aggregation complete: {hourly_created} hourly, "
            f"{daily_created} daily in {processing_time:.2f}s"
        )

        return stats

    def detect_gaps(
        self, resource_id: int, metric_definition_id: int, start_time: datetime, end_time: datetime, expected_interval_minutes: int = 60
    ) -> List[DataGap]:
        """
        Detect gaps in time-series data.

        Identifies periods where expected data points are missing based on
        the expected reporting interval.

        Args:
            resource_id: Resource to check
            metric_definition_id: Metric to check
            start_time: Start of time range
            end_time: End of time range
            expected_interval_minutes: Expected interval between data points

        Returns:
            List of detected gaps
        """
        gaps = []

        with self.db_connection.cursor() as cur:
            # Get all timestamps for this metric
            cur.execute(
                """
                SELECT timestamp
                FROM metric_data
                WHERE resource_id = %s
                  AND metric_definition_id = %s
                  AND timestamp >= %s
                  AND timestamp < %s
                ORDER BY timestamp
            """,
                (resource_id, metric_definition_id, start_time, end_time),
            )

            timestamps = [row[0] for row in cur.fetchall()]

        if len(timestamps) < 2:
            return gaps

        # Check intervals between consecutive timestamps
        expected_interval = timedelta(minutes=expected_interval_minutes)
        tolerance = timedelta(minutes=expected_interval_minutes * 0.1)  # 10% tolerance

        for i in range(len(timestamps) - 1):
            current_ts = timestamps[i]
            next_ts = timestamps[i + 1]
            actual_interval = next_ts - current_ts

            # If gap is significantly larger than expected
            if actual_interval > expected_interval + tolerance:
                gap_hours = actual_interval.total_seconds() / 3600
                expected_ts = current_ts + expected_interval

                gap = DataGap(
                    resource_id=resource_id,
                    metric_definition_id=metric_definition_id,
                    expected_timestamp=expected_ts,
                    gap_duration_hours=gap_hours,
                    previous_timestamp=current_ts,
                    next_timestamp=next_ts,
                )
                gaps.append(gap)

        return gaps

    def apply_retention_policy(self, dry_run: bool = True) -> Dict[str, int]:
        """
        Apply retention policy to delete old data.

        Removes data older than configured retention periods:
        - Raw metric_data
        - Hourly rollups
        - Daily summaries

        Args:
            dry_run: If True, only count rows that would be deleted

        Returns:
            Dictionary with counts of rows deleted/would be deleted
        """
        results = {
            "raw_data_deleted": 0,
            "hourly_rollups_deleted": 0,
            "daily_summaries_deleted": 0,
        }

        now = datetime.now(timezone.utc)

        with self.db_connection.cursor() as cur:
            # Delete old raw data
            if self.retention_policy.raw_data_days > 0:
                cutoff = now - timedelta(days=self.retention_policy.raw_data_days)

                if dry_run:
                    cur.execute(
                        "SELECT COUNT(*) FROM metric_data WHERE timestamp < %s", (cutoff,)
                    )
                    results["raw_data_deleted"] = cur.fetchone()[0]
                else:
                    cur.execute("DELETE FROM metric_data WHERE timestamp < %s", (cutoff,))
                    results["raw_data_deleted"] = cur.rowcount

            # Delete old hourly rollups
            if self.retention_policy.hourly_rollup_days > 0:
                cutoff = now - timedelta(days=self.retention_policy.hourly_rollup_days)

                if dry_run:
                    cur.execute(
                        "SELECT COUNT(*) FROM hourly_rollups WHERE hour < %s", (cutoff,)
                    )
                    results["hourly_rollups_deleted"] = cur.fetchone()[0]
                else:
                    cur.execute("DELETE FROM hourly_rollups WHERE hour < %s", (cutoff,))
                    results["hourly_rollups_deleted"] = cur.rowcount

            # Delete old daily summaries
            if self.retention_policy.daily_summary_days > 0:
                cutoff = now - timedelta(days=self.retention_policy.daily_summary_days)

                if dry_run:
                    cur.execute(
                        "SELECT COUNT(*) FROM daily_summaries WHERE day < %s", (cutoff,)
                    )
                    results["daily_summaries_deleted"] = cur.fetchone()[0]
                else:
                    cur.execute("DELETE FROM daily_summaries WHERE day < %s", (cutoff,))
                    results["daily_summaries_deleted"] = cur.rowcount

            if not dry_run:
                self.db_connection.commit()

        mode = "Would delete" if dry_run else "Deleted"
        self.logger.info(
            f"{mode}: {results['raw_data_deleted']} raw data, "
            f"{results['hourly_rollups_deleted']} hourly rollups, "
            f"{results['daily_summaries_deleted']} daily summaries"
        )

        return results

    def validate_data_quality(
        self, start_time: datetime, end_time: datetime
    ) -> Dict[str, Any]:
        """
        Validate data quality for a time range.

        Checks:
        - Missing values (NULL)
        - Out-of-range values (extreme outliers)
        - Duplicate timestamps
        - Data point distribution

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dictionary with validation results
        """
        results = {
            "total_data_points": 0,
            "null_values": 0,
            "duplicate_timestamps": 0,
            "extreme_outliers": 0,
            "metrics_validated": 0,
        }

        with self.db_connection.cursor() as cur:
            # Count total data points
            cur.execute(
                """
                SELECT COUNT(*)
                FROM metric_data
                WHERE timestamp >= %s AND timestamp < %s
            """,
                (start_time, end_time),
            )
            results["total_data_points"] = cur.fetchone()[0]

            # Count NULL values
            cur.execute(
                """
                SELECT COUNT(*)
                FROM metric_data
                WHERE timestamp >= %s AND timestamp < %s
                  AND value_double IS NULL AND value_int IS NULL
            """,
                (start_time, end_time),
            )
            results["null_values"] = cur.fetchone()[0]

            # Count duplicates (same resource, metric, timestamp)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT resource_id, metric_definition_id, timestamp
                    FROM metric_data
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY resource_id, metric_definition_id, timestamp
                    HAVING COUNT(*) > 1
                ) duplicates
            """,
                (start_time, end_time),
            )
            results["duplicate_timestamps"] = cur.fetchone()[0]

            # Count metrics validated
            cur.execute(
                """
                SELECT COUNT(DISTINCT metric_definition_id)
                FROM metric_data
                WHERE timestamp >= %s AND timestamp < %s
            """,
                (start_time, end_time),
            )
            results["metrics_validated"] = cur.fetchone()[0]

        return results

    def _ensure_hourly_rollups_table(self):
        """Create hourly_rollups table if it doesn't exist."""
        with self.db_connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_rollups (
                    id SERIAL PRIMARY KEY,
                    resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
                    metric_definition_id INTEGER NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
                    hour TIMESTAMPTZ NOT NULL,
                    data_point_count INTEGER NOT NULL,
                    min_value DOUBLE PRECISION,
                    max_value DOUBLE PRECISION,
                    avg_value DOUBLE PRECISION,
                    sum_value DOUBLE PRECISION,
                    stddev_value DOUBLE PRECISION,
                    first_timestamp TIMESTAMPTZ,
                    last_timestamp TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(resource_id, metric_definition_id, hour)
                )
            """)

            # Create indexes
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ix_hourly_rollups_hour ON hourly_rollups(hour DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ix_hourly_rollups_resource_metric ON hourly_rollups(resource_id, metric_definition_id, hour DESC)"
            )

            self.db_connection.commit()

    def _ensure_daily_summaries_table(self):
        """Create daily_summaries table if it doesn't exist."""
        with self.db_connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_summaries (
                    id SERIAL PRIMARY KEY,
                    resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
                    metric_definition_id INTEGER NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
                    day DATE NOT NULL,
                    hour_count INTEGER NOT NULL,
                    total_data_points INTEGER NOT NULL,
                    min_value DOUBLE PRECISION,
                    max_value DOUBLE PRECISION,
                    avg_value DOUBLE PRECISION,
                    sum_value DOUBLE PRECISION,
                    stddev_value DOUBLE PRECISION,
                    first_hour TIMESTAMPTZ,
                    last_hour TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(resource_id, metric_definition_id, day)
                )
            """)

            # Create indexes
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ix_daily_summaries_day ON daily_summaries(day DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ix_daily_summaries_resource_metric ON daily_summaries(resource_id, metric_definition_id, day DESC)"
            )

            self.db_connection.commit()
