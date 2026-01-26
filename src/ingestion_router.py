# Description: Ingestion router for dual-write to Data Lake and hot cache
# Description: Handles parsing and routing of OTLP data to appropriate stores

"""
Ingestion Router for Dual-Write Architecture.

Routes parsed OTLP data to:
1. Azure Data Lake Gen2 (all data, Parquet) - for ML training and historical queries
2. PostgreSQL hot cache (recent data) - for real-time dashboards

Replaces the async processing backlog with direct writes.
"""

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg

from src.datalake_writer import DataLakeWriter
from src.otlp_parser import (
    ParsedOTLP,
    deduplicate_datasources,
    deduplicate_metric_definitions,
    deduplicate_resources,
    parse_otlp,
)

logger = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    """Configuration for ingestion routing."""
    write_to_datalake: bool = True
    write_to_hot_cache: bool = True

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        """Create config from environment variables."""
        return cls(
            write_to_datalake=os.getenv("WRITE_TO_DATALAKE", "true").lower() == "true",
            write_to_hot_cache=os.getenv("WRITE_TO_HOT_CACHE", "true").lower() == "true",
        )


@dataclass
class IngestionStats:
    """Statistics from an ingestion operation."""
    resources: int
    datasources: int
    metric_definitions: int
    metric_data: int
    datalake_written: int
    hot_cache_written: int
    errors: list

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "resources": self.resources,
            "datasources": self.datasources,
            "metric_definitions": self.metric_definitions,
            "metric_data": self.metric_data,
            "datalake_written": self.datalake_written,
            "hot_cache_written": self.hot_cache_written,
            "errors": self.errors,
        }


class IngestionRouter:
    """
    Routes ingested OTLP data to Data Lake and hot cache.

    This is the main entry point for data ingestion, replacing the previous
    lm_metrics + async processing approach with direct dual-writes.
    """

    def __init__(
        self,
        datalake_writer: Optional[DataLakeWriter] = None,
        db_pool: Optional[asyncpg.Pool] = None,
        config: Optional[IngestionConfig] = None,
    ):
        """
        Initialize the ingestion router.

        Args:
            datalake_writer: DataLakeWriter for Parquet writes (optional)
            db_pool: asyncpg pool for hot cache writes (optional)
            config: Ingestion configuration
        """
        self.datalake_writer = datalake_writer
        self.db_pool = db_pool
        self.config = config or IngestionConfig.from_env()

    async def ingest(self, payload: Dict[str, Any]) -> IngestionStats:
        """
        Ingest OTLP payload to Data Lake and optionally hot cache.

        Args:
            payload: Raw OTLP JSON payload

        Returns:
            IngestionStats with counts and any errors
        """
        errors = []

        # Parse OTLP payload
        try:
            parsed = parse_otlp(payload)
        except Exception as e:
            logger.error(f"Failed to parse OTLP payload: {e}")
            return IngestionStats(
                resources=0,
                datasources=0,
                metric_definitions=0,
                metric_data=0,
                datalake_written=0,
                hot_cache_written=0,
                errors=[f"Parse error: {str(e)}"],
            )

        # Deduplicate
        unique_resources = deduplicate_resources(parsed.resources)
        unique_datasources = deduplicate_datasources(parsed.datasources)
        unique_metric_defs = deduplicate_metric_definitions(parsed.metric_definitions)

        stats = IngestionStats(
            resources=len(unique_resources),
            datasources=len(unique_datasources),
            metric_definitions=len(unique_metric_defs),
            metric_data=len(parsed.metric_data),
            datalake_written=0,
            hot_cache_written=0,
            errors=[],
        )

        # Write to Data Lake (all data, for ML and historical queries)
        if self.config.write_to_datalake and self.datalake_writer:
            try:
                stats.datalake_written = await self.datalake_writer.write_metrics(parsed)
            except Exception as e:
                error_msg = f"Data Lake write error: {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)

        # Write to hot cache (for real-time dashboards)
        if self.config.write_to_hot_cache and self.db_pool:
            try:
                stats.hot_cache_written = await self._write_to_hot_cache(parsed)
            except Exception as e:
                error_msg = f"Hot cache write error: {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)

        stats.errors = errors

        if stats.datalake_written > 0 or stats.hot_cache_written > 0:
            logger.info(
                f"Ingested: {stats.metric_data} data points "
                f"(datalake: {stats.datalake_written}, hot_cache: {stats.hot_cache_written})"
            )

        return stats

    async def _write_to_hot_cache(self, parsed: ParsedOTLP) -> int:
        """
        Write parsed data to PostgreSQL hot cache.

        This uses a simplified write path that stores normalized data
        directly, without the intermediate lm_metrics table.

        Args:
            parsed: ParsedOTLP with deduplicated data

        Returns:
            Number of metric data points written
        """
        if not self.db_pool:
            return 0

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                # Upsert resources
                resource_id_map = {}
                for resource in parsed.resources:
                    resource_id = await conn.fetchval("""
                        INSERT INTO resources (resource_hash, attributes)
                        VALUES ($1, $2)
                        ON CONFLICT (resource_hash) DO UPDATE
                            SET updated_at = NOW()
                        RETURNING id
                    """, resource.resource_hash, json.dumps(resource.attributes))
                    resource_id_map[resource.resource_hash] = resource_id

                # Upsert datasources
                datasource_id_map = {}
                for ds in parsed.datasources:
                    # Try insert first
                    result = await conn.fetchrow("""
                        INSERT INTO datasources (name, version)
                        VALUES ($1, $2)
                        ON CONFLICT (name, version) DO NOTHING
                        RETURNING id
                    """, ds.name, ds.version)

                    if result:
                        ds_id = result['id']
                    else:
                        # Get existing
                        ds_id = await conn.fetchval("""
                            SELECT id FROM datasources
                            WHERE name = $1 AND version IS NOT DISTINCT FROM $2
                        """, ds.name, ds.version)

                    datasource_id_map[(ds.name, ds.version)] = ds_id

                # Upsert metric definitions
                metric_def_id_map = {}
                for md in parsed.metric_definitions:
                    ds_id = datasource_id_map.get((md.datasource_name, md.datasource_version))
                    if not ds_id:
                        continue

                    result = await conn.fetchrow("""
                        INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (datasource_id, name) DO NOTHING
                        RETURNING id
                    """, ds_id, md.name, md.unit, md.metric_type, md.description)

                    if result:
                        md_id = result['id']
                    else:
                        md_id = await conn.fetchval("""
                            SELECT id FROM metric_definitions
                            WHERE datasource_id = $1 AND name = $2
                        """, ds_id, md.name)

                    metric_def_id_map[(md.datasource_name, md.datasource_version, md.name)] = md_id

                # Batch insert metric data points
                batch_data = []
                for dp in parsed.metric_data:
                    resource_id = resource_id_map.get(dp.resource_hash)
                    metric_def_id = metric_def_id_map.get(
                        (dp.datasource_name, dp.datasource_version, dp.metric_name)
                    )

                    if resource_id and metric_def_id:
                        batch_data.append((
                            resource_id,
                            metric_def_id,
                            dp.timestamp,
                            dp.value_double,
                            dp.value_int,
                            json.dumps(dp.attributes) if dp.attributes else None,
                        ))

                if batch_data:
                    await conn.executemany("""
                        INSERT INTO metric_data (
                            resource_id, metric_definition_id, timestamp,
                            value_double, value_int, attributes
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, batch_data)

                return len(batch_data)

    async def get_status(self) -> Dict[str, Any]:
        """Get current status of the ingestion router."""
        status = {
            "config": {
                "write_to_datalake": self.config.write_to_datalake,
                "write_to_hot_cache": self.config.write_to_hot_cache,
            },
            "datalake": None,
            "hot_cache": None,
        }

        if self.datalake_writer:
            status["datalake"] = self.datalake_writer.get_buffer_stats()

        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    count = await conn.fetchval("SELECT COUNT(*) FROM metric_data")
                    status["hot_cache"] = {"metric_data_count": count}
            except Exception as e:
                status["hot_cache"] = {"error": str(e)}

        return status
