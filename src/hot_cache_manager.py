# Description: PostgreSQL hot cache manager for real-time dashboard data
# Description: Maintains last 24-48 hours of data with automatic TTL cleanup

"""
Hot Cache Manager for PostgreSQL.

Manages the hot cache (PostgreSQL) containing recent data for real-time
dashboards. Handles TTL-based cleanup and provides cache statistics.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import asyncpg

logger = logging.getLogger(__name__)


class HotCacheManager:
    """
    Manages PostgreSQL hot cache for real-time data.

    The hot cache stores recent metric data (default 48 hours) for fast
    dashboard queries. Older data is automatically cleaned up.
    """

    DEFAULT_RETENTION_HOURS = 48

    def __init__(
        self,
        pool: asyncpg.Pool,
        retention_hours: Optional[int] = None,
    ):
        """
        Initialize the hot cache manager.

        Args:
            pool: asyncpg connection pool
            retention_hours: Hours to retain data (default from env or 48)
        """
        self.pool = pool
        self.retention_hours = retention_hours or int(
            os.getenv("HOT_CACHE_RETENTION_HOURS", str(self.DEFAULT_RETENTION_HOURS))
        )

    async def cleanup_expired_data(self) -> Dict[str, int]:
        """
        Delete data older than retention period.

        Returns:
            Dict with counts of deleted records per table
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        deleted_counts = {}

        async with self.pool.acquire() as conn:
            # Delete expired metric_data
            result = await conn.execute("""
                DELETE FROM metric_data
                WHERE timestamp < $1
            """, cutoff)
            # Parse "DELETE N" to get count
            deleted_counts['metric_data'] = int(result.split()[-1]) if result else 0

            # Delete orphaned processing_status records
            result = await conn.execute("""
                DELETE FROM processing_status
                WHERE created_at < $1
            """, cutoff)
            deleted_counts['processing_status'] = int(result.split()[-1]) if result else 0

            # Delete old raw lm_metrics (after processing)
            result = await conn.execute("""
                DELETE FROM lm_metrics
                WHERE ingested_at < $1
            """, cutoff)
            deleted_counts['lm_metrics'] = int(result.split()[-1]) if result else 0

        total_deleted = sum(deleted_counts.values())
        if total_deleted > 0:
            logger.info(
                f"Hot cache cleanup: deleted {total_deleted} records "
                f"older than {cutoff.isoformat()} "
                f"(metric_data: {deleted_counts['metric_data']}, "
                f"processing_status: {deleted_counts['processing_status']}, "
                f"lm_metrics: {deleted_counts['lm_metrics']})"
            )

        return deleted_counts

    async def get_cache_stats(self) -> Dict[str, any]:
        """
        Get statistics about hot cache.

        Returns:
            Dict with cache statistics
        """
        async with self.pool.acquire() as conn:
            # Get metric_data stats
            metric_stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_rows,
                    MIN(timestamp) as oldest,
                    MAX(timestamp) as newest,
                    pg_size_pretty(pg_total_relation_size('metric_data')) as table_size
                FROM metric_data
            """)

            # Get processing stats
            processing_stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'success') as successful,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'processing') as in_progress
                FROM processing_status
            """)

            # Get pending count (lm_metrics without processing_status)
            pending = await conn.fetchval("""
                SELECT COUNT(*)
                FROM lm_metrics lm
                WHERE NOT EXISTS (
                    SELECT 1 FROM processing_status ps
                    WHERE ps.lm_metrics_id = lm.id
                )
            """)

            return {
                "metric_data": {
                    "total_rows": metric_stats["total_rows"],
                    "oldest_timestamp": metric_stats["oldest"].isoformat() if metric_stats["oldest"] else None,
                    "newest_timestamp": metric_stats["newest"].isoformat() if metric_stats["newest"] else None,
                    "table_size": metric_stats["table_size"],
                },
                "processing": {
                    "total": processing_stats["total"],
                    "successful": processing_stats["successful"],
                    "failed": processing_stats["failed"],
                    "in_progress": processing_stats["in_progress"],
                    "pending": pending,
                },
                "retention_hours": self.retention_hours,
                "cutoff_time": (
                    datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
                ).isoformat(),
            }

    async def is_healthy(self) -> bool:
        """
        Check if hot cache is healthy.

        Returns:
            True if cache is accessible and not too far behind
        """
        try:
            async with self.pool.acquire() as conn:
                # Check if we can query
                newest = await conn.fetchval("""
                    SELECT MAX(timestamp) FROM metric_data
                """)

                if newest is None:
                    # Empty cache is OK
                    return True

                # Check if newest data is within acceptable range (2x retention)
                age = datetime.now(timezone.utc) - newest
                max_acceptable_age = timedelta(hours=self.retention_hours * 2)

                return age < max_acceptable_age

        except Exception as e:
            logger.error(f"Hot cache health check failed: {e}")
            return False
