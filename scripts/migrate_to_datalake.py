# Description: Migration script to backfill PostgreSQL data to Azure Data Lake Gen2
# Description: Run once to migrate historical metric data to Parquet format

"""
PostgreSQL to Data Lake Migration Script.

Migrates existing metric_data from PostgreSQL to Azure Data Lake Gen2 as Parquet files.
This is a one-time migration script to backfill historical data.

Usage:
    uv run python scripts/migrate_to_datalake.py [--batch-size 10000] [--dry-run]

Environment variables required:
    - POSTGRES_* (database connection)
    - DATALAKE_* (Data Lake connection)
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict, List, Any
import uuid

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from src.datalake_writer import (
    METRIC_DATA_SCHEMA,
    RESOURCES_SCHEMA,
    DATASOURCES_SCHEMA,
    METRIC_DEFINITIONS_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLakeMigrator:
    """Migrates PostgreSQL data to Azure Data Lake Gen2."""

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        datalake_account: str,
        datalake_filesystem: str,
        datalake_base_path: str = "otlp",
        batch_size: int = 10000,
        dry_run: bool = False,
    ):
        self.db_pool = db_pool
        self.datalake_account = datalake_account
        self.datalake_filesystem = datalake_filesystem
        self.datalake_base_path = datalake_base_path
        self.batch_size = batch_size
        self.dry_run = dry_run
        self._service_client = None
        self._credential = None

    def _get_service_client(self) -> DataLakeServiceClient:
        """Get or create the Data Lake service client."""
        if self._service_client is None:
            self._credential = DefaultAzureCredential()
            self._service_client = DataLakeServiceClient(
                account_url=f"https://{self.datalake_account}.dfs.core.windows.net",
                credential=self._credential
            )
        return self._service_client

    async def get_migration_stats(self) -> Dict[str, int]:
        """Get counts of data to migrate."""
        async with self.db_pool.acquire() as conn:
            stats = {}
            stats["resources"] = await conn.fetchval("SELECT COUNT(*) FROM resources")
            stats["datasources"] = await conn.fetchval("SELECT COUNT(*) FROM datasources")
            stats["metric_definitions"] = await conn.fetchval("SELECT COUNT(*) FROM metric_definitions")
            stats["metric_data"] = await conn.fetchval("SELECT COUNT(*) FROM metric_data")

            # Get time range
            time_range = await conn.fetchrow("""
                SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
                FROM metric_data
            """)
            stats["min_timestamp"] = time_range["min_ts"].isoformat() if time_range["min_ts"] else None
            stats["max_timestamp"] = time_range["max_ts"].isoformat() if time_range["max_ts"] else None

        return stats

    async def migrate_resources(self) -> int:
        """Migrate resources table to Parquet."""
        logger.info("Migrating resources...")

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    resource_hash,
                    attributes::text as attributes,
                    created_at,
                    updated_at
                FROM resources
            """)

        if not rows:
            logger.info("No resources to migrate")
            return 0

        data = [
            {
                "resource_hash": row["resource_hash"],
                "attributes": row["attributes"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"] or row["created_at"],
            }
            for row in rows
        ]

        if self.dry_run:
            logger.info(f"[DRY RUN] Would write {len(data)} resources")
            return len(data)

        table = pa.Table.from_pylist(data, schema=RESOURCES_SCHEMA)
        self._write_parquet(table, f"{self.datalake_base_path}/resources/resources-migration.parquet")

        logger.info(f"Migrated {len(data)} resources")
        return len(data)

    async def migrate_datasources(self) -> int:
        """Migrate datasources table to Parquet."""
        logger.info("Migrating datasources...")

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT name, version, created_at
                FROM datasources
            """)

        if not rows:
            logger.info("No datasources to migrate")
            return 0

        data = [
            {
                "name": row["name"],
                "version": row["version"],
                "created_at": row["created_at"] or datetime.now(timezone.utc),
            }
            for row in rows
        ]

        if self.dry_run:
            logger.info(f"[DRY RUN] Would write {len(data)} datasources")
            return len(data)

        table = pa.Table.from_pylist(data, schema=DATASOURCES_SCHEMA)
        self._write_parquet(table, f"{self.datalake_base_path}/datasources/datasources-migration.parquet")

        logger.info(f"Migrated {len(data)} datasources")
        return len(data)

    async def migrate_metric_definitions(self) -> int:
        """Migrate metric_definitions table to Parquet."""
        logger.info("Migrating metric definitions...")

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    md.name,
                    md.unit,
                    md.metric_type,
                    md.description,
                    md.created_at,
                    ds.name as datasource_name,
                    ds.version as datasource_version
                FROM metric_definitions md
                JOIN datasources ds ON md.datasource_id = ds.id
            """)

        if not rows:
            logger.info("No metric definitions to migrate")
            return 0

        data = [
            {
                "datasource_name": row["datasource_name"],
                "datasource_version": row["datasource_version"],
                "name": row["name"],
                "unit": row["unit"],
                "metric_type": row["metric_type"],
                "description": row["description"],
                "created_at": row["created_at"] or datetime.now(timezone.utc),
            }
            for row in rows
        ]

        if self.dry_run:
            logger.info(f"[DRY RUN] Would write {len(data)} metric definitions")
            return len(data)

        table = pa.Table.from_pylist(data, schema=METRIC_DEFINITIONS_SCHEMA)
        self._write_parquet(
            table,
            f"{self.datalake_base_path}/metric_definitions/metric_definitions-migration.parquet"
        )

        logger.info(f"Migrated {len(data)} metric definitions")
        return len(data)

    async def migrate_metric_data(self) -> int:
        """Migrate metric_data table to Parquet in batches."""
        logger.info("Migrating metric data...")

        total_migrated = 0
        offset = 0

        while True:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT
                        r.resource_hash,
                        ds.name as datasource_name,
                        md.name as metric_name,
                        m.timestamp,
                        m.value_double,
                        m.value_int,
                        m.attributes::text as attributes
                    FROM metric_data m
                    JOIN resources r ON m.resource_id = r.id
                    JOIN metric_definitions md ON m.metric_definition_id = md.id
                    JOIN datasources ds ON md.datasource_id = ds.id
                    ORDER BY m.timestamp
                    LIMIT $1 OFFSET $2
                """, self.batch_size, offset)

            if not rows:
                break

            # Group by partition (year/month/day/hour)
            partitions: Dict[str, List[Dict]] = {}
            for row in rows:
                ts = row["timestamp"]
                partition_key = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"

                data_row = {
                    "resource_hash": row["resource_hash"],
                    "datasource_name": row["datasource_name"],
                    "metric_name": row["metric_name"],
                    "timestamp": ts,
                    "value_double": row["value_double"],
                    "value_int": row["value_int"],
                    "attributes": row["attributes"],
                    "ingested_at": datetime.now(timezone.utc),
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "hour": ts.hour,
                }
                partitions.setdefault(partition_key, []).append(data_row)

            if self.dry_run:
                logger.info(f"[DRY RUN] Would write {len(rows)} records across {len(partitions)} partitions")
            else:
                # Write each partition
                for partition_path, data in partitions.items():
                    table = pa.Table.from_pylist(data, schema=METRIC_DATA_SCHEMA)
                    filename = f"migration-{uuid.uuid4().hex[:8]}.parquet"
                    full_path = f"{self.datalake_base_path}/metric_data/{partition_path}/{filename}"
                    self._write_parquet(table, full_path)

            total_migrated += len(rows)
            offset += self.batch_size

            logger.info(f"Migrated {total_migrated} metric data points...")

            # Stop if we got fewer than batch_size (last batch)
            if len(rows) < self.batch_size:
                break

        logger.info(f"Total metric data migrated: {total_migrated}")
        return total_migrated

    def _write_parquet(self, table: pa.Table, path: str):
        """Write a PyArrow table to Data Lake as Parquet."""
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        service_client = self._get_service_client()
        fs_client = service_client.get_file_system_client(self.datalake_filesystem)
        file_client = fs_client.get_file_client(path)
        file_client.upload_data(buffer.getvalue(), overwrite=True)

        logger.debug(f"Wrote {path}")

    async def run_migration(self) -> Dict[str, int]:
        """Run the full migration."""
        logger.info("=" * 60)
        logger.info("PostgreSQL to Data Lake Migration")
        logger.info("=" * 60)

        if self.dry_run:
            logger.info("DRY RUN MODE - No data will be written")

        # Get stats
        stats = await self.get_migration_stats()
        logger.info(f"Source data statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")

        logger.info("-" * 60)

        results = {}

        # Migrate in order (resources first, then dependencies)
        results["resources"] = await self.migrate_resources()
        results["datasources"] = await self.migrate_datasources()
        results["metric_definitions"] = await self.migrate_metric_definitions()
        results["metric_data"] = await self.migrate_metric_data()

        logger.info("=" * 60)
        logger.info("Migration complete!")
        logger.info(f"Results: {results}")
        logger.info("=" * 60)

        return results


async def main():
    parser = argparse.ArgumentParser(description="Migrate PostgreSQL data to Data Lake")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for metric_data")
    parser.add_argument("--dry-run", action="store_true", help="Don't write data, just show what would happen")
    args = parser.parse_args()

    # Get configuration from environment
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "postgres")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "")

    datalake_account = os.getenv("DATALAKE_ACCOUNT", "stlmingestdatalake")
    datalake_filesystem = os.getenv("DATALAKE_FILESYSTEM", "metrics")
    datalake_base_path = os.getenv("DATALAKE_BASE_PATH", "otlp")

    logger.info(f"Connecting to PostgreSQL: {db_host}:{db_port}/{db_name}")
    logger.info(f"Target Data Lake: {datalake_account}/{datalake_filesystem}/{datalake_base_path}")

    # Create database pool
    db_pool = await asyncpg.create_pool(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
        ssl="require" if db_host != "localhost" else None,
        min_size=2,
        max_size=10,
    )

    try:
        migrator = DataLakeMigrator(
            db_pool=db_pool,
            datalake_account=datalake_account,
            datalake_filesystem=datalake_filesystem,
            datalake_base_path=datalake_base_path,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )

        await migrator.run_migration()

    finally:
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
