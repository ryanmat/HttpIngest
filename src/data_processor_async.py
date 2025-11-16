# ABOUTME: Async data processor for converting raw lm_metrics to normalized OTLP tables
# ABOUTME: Uses asyncpg for non-blocking database operations with connection pooling

"""
Async Data Processor for LogicMonitor OTLP Pipeline

Processes raw lm_metrics records into normalized tables:
- resources
- datasources
- metric_definitions
- metric_data
- processing_status

Features:
- Async/await with asyncpg (non-blocking)
- Connection pooling for performance
- Idempotent processing (can reprocess records)
- Transaction safety (rollback on errors)
- Batch processing for performance
- Comprehensive error handling and logging
"""

import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass
import asyncpg

from src.otlp_parser import (
    parse_otlp,
    deduplicate_resources,
    deduplicate_datasources,
    deduplicate_metric_definitions,
    ResourceData,
    DatasourceData,
    MetricDefinitionData,
    MetricDataPoint,
    ParsedOTLP
)

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of processing a single lm_metrics record."""
    lm_metrics_id: int
    success: bool
    resources_created: int
    datasources_created: int
    metric_definitions_created: int
    metric_data_created: int
    error_message: Optional[str] = None


@dataclass
class BatchProcessingStats:
    """Statistics for batch processing."""
    total_records: int
    successful: int
    failed: int
    resources_created: int
    datasources_created: int
    metric_definitions_created: int
    metric_data_created: int
    errors: List[str]


class AsyncDataProcessor:
    """
    Async processor for raw lm_metrics records into normalized OTLP tables.

    Handles the complete pipeline:
    1. Query unprocessed records
    2. Parse OTLP data
    3. Upsert resources and datasources
    4. Insert metric definitions and data
    5. Update processing status

    All operations are async and use connection pooling.
    """

    def __init__(self, db_pool: asyncpg.Pool):
        """
        Initialize async data processor.

        Args:
            db_pool: asyncpg connection pool
        """
        self.db_pool = db_pool
        self.logger = logging.getLogger(__name__)

    async def get_unprocessed_records(self, limit: Optional[int] = None) -> List[Tuple[int, Dict[str, Any]]]:
        """
        Query unprocessed records from lm_metrics.

        Args:
            limit: Maximum number of records to fetch (None for all)

        Returns:
            List of (id, payload) tuples
        """
        query = """
            SELECT lm.id, lm.payload
            FROM lm_metrics lm
            LEFT JOIN processing_status ps ON lm.id = ps.lm_metrics_id
            WHERE ps.id IS NULL OR ps.status = 'failed'
            ORDER BY lm.id
        """

        if limit:
            query += f" LIMIT {limit}"

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            # Parse JSON string to dict if needed
            result = []
            for row in rows:
                payload = row['payload']
                if isinstance(payload, str):
                    payload = json.loads(payload)
                result.append((row['id'], payload))
            return result

    async def upsert_resource(self, conn: asyncpg.Connection, resource: ResourceData) -> int:
        """
        Upsert resource into resources table.

        Args:
            conn: Database connection
            resource: ResourceData object

        Returns:
            Resource ID (existing or newly created)
        """
        resource_id = await conn.fetchval("""
            INSERT INTO resources (resource_hash, attributes)
            VALUES ($1, $2)
            ON CONFLICT (resource_hash) DO UPDATE
                SET updated_at = NOW()
            RETURNING id
        """, resource.resource_hash, json.dumps(resource.attributes))

        return resource_id

    async def upsert_datasource(self, conn: asyncpg.Connection, datasource: DatasourceData) -> int:
        """
        Upsert datasource into datasources table.

        Args:
            conn: Database connection
            datasource: DatasourceData object

        Returns:
            Datasource ID (existing or newly created)
        """
        # Try to insert
        result = await conn.fetchrow("""
            INSERT INTO datasources (name, version)
            VALUES ($1, $2)
            ON CONFLICT (name, version) DO NOTHING
            RETURNING id
        """, datasource.name, datasource.version)

        if result:
            return result['id']

        # If conflict, get existing ID
        existing = await conn.fetchrow("""
            SELECT id FROM datasources
            WHERE name = $1 AND version IS NOT DISTINCT FROM $2
        """, datasource.name, datasource.version)

        return existing['id']

    async def upsert_metric_definition(
        self,
        conn: asyncpg.Connection,
        metric_def: MetricDefinitionData,
        datasource_id: int
    ) -> int:
        """
        Upsert metric definition into metric_definitions table.

        Args:
            conn: Database connection
            metric_def: MetricDefinitionData object
            datasource_id: ID of the datasource

        Returns:
            Metric definition ID (existing or newly created)
        """
        # Try to insert
        result = await conn.fetchrow("""
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (datasource_id, name) DO NOTHING
            RETURNING id
        """, datasource_id, metric_def.name, metric_def.unit, metric_def.metric_type, metric_def.description)

        if result:
            return result['id']

        # If conflict, get existing ID
        existing = await conn.fetchrow("""
            SELECT id FROM metric_definitions
            WHERE datasource_id = $1 AND name = $2
        """, datasource_id, metric_def.name)

        return existing['id']

    async def insert_metric_data_point(
        self,
        conn: asyncpg.Connection,
        data_point: MetricDataPoint,
        resource_id: int,
        metric_definition_id: int
    ) -> int:
        """
        Insert metric data point into metric_data table.

        Args:
            conn: Database connection
            data_point: MetricDataPoint object
            resource_id: ID of the resource
            metric_definition_id: ID of the metric definition

        Returns:
            Metric data ID
        """
        metric_data_id = await conn.fetchval("""
            INSERT INTO metric_data (
                resource_id,
                metric_definition_id,
                timestamp,
                value_double,
                value_int,
                attributes
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """,
            resource_id,
            metric_definition_id,
            data_point.timestamp,
            data_point.value_double,
            data_point.value_int,
            json.dumps(data_point.attributes) if data_point.attributes else None
        )

        return metric_data_id

    async def mark_as_processing(self, conn: asyncpg.Connection, lm_metrics_id: int) -> None:
        """
        Mark a record as being processed.

        Args:
            conn: Database connection
            lm_metrics_id: ID of the lm_metrics record
        """
        await conn.execute("""
            INSERT INTO processing_status (lm_metrics_id, status)
            VALUES ($1, 'processing')
            ON CONFLICT (lm_metrics_id) DO UPDATE
                SET status = 'processing',
                    updated_at = NOW()
        """, lm_metrics_id)

    async def mark_as_success(self, conn: asyncpg.Connection, lm_metrics_id: int, metrics_extracted: int) -> None:
        """
        Mark a record as successfully processed.

        Args:
            conn: Database connection
            lm_metrics_id: ID of the lm_metrics record
            metrics_extracted: Number of metric data points extracted
        """
        await conn.execute("""
            INSERT INTO processing_status (lm_metrics_id, status, processed_at, metrics_extracted)
            VALUES ($1, 'success', NOW(), $2)
            ON CONFLICT (lm_metrics_id) DO UPDATE
                SET status = 'success',
                    processed_at = NOW(),
                    metrics_extracted = $2,
                    error_message = NULL,
                    updated_at = NOW()
        """, lm_metrics_id, metrics_extracted)

    async def mark_as_failed(self, conn: asyncpg.Connection, lm_metrics_id: int, error_message: str) -> None:
        """
        Mark a record as failed.

        Args:
            conn: Database connection
            lm_metrics_id: ID of the lm_metrics record
            error_message: Error message describing the failure
        """
        # Truncate error message if too long
        error_message = error_message[:1000] if len(error_message) > 1000 else error_message

        await conn.execute("""
            INSERT INTO processing_status (lm_metrics_id, status, processed_at, error_message)
            VALUES ($1, 'failed', NOW(), $2)
            ON CONFLICT (lm_metrics_id) DO UPDATE
                SET status = 'failed',
                    processed_at = NOW(),
                    error_message = $2,
                    updated_at = NOW()
        """, lm_metrics_id, error_message)

    async def process_single_record(self, lm_metrics_id: int, payload: Dict[str, Any]) -> ProcessingResult:
        """
        Process a single lm_metrics record.

        This method is transactional - if any step fails, all changes are rolled back.

        Args:
            lm_metrics_id: ID of the lm_metrics record
            payload: OTLP JSON payload

        Returns:
            ProcessingResult with statistics and status
        """
        async with self.db_pool.acquire() as conn:
            try:
                # Start transaction
                async with conn.transaction():
                    # Mark as processing
                    await self.mark_as_processing(conn, lm_metrics_id)

                    # Parse OTLP payload
                    self.logger.debug(f"Parsing record {lm_metrics_id}")
                    parsed = parse_otlp(payload)

                    # Deduplicate
                    unique_resources = deduplicate_resources(parsed.resources)
                    unique_datasources = deduplicate_datasources(parsed.datasources)
                    unique_metric_defs = deduplicate_metric_definitions(parsed.metric_definitions)

                    resources_created = 0
                    datasources_created = 0
                    metric_defs_created = 0
                    metric_data_created = 0

                    # Track resource IDs for metric data insertion
                    resource_id_map = {}  # resource_hash -> resource_id

                    # Upsert resources
                    self.logger.debug(f"Upserting {len(unique_resources)} resources")
                    for resource in unique_resources:
                        resource_id = await self.upsert_resource(conn, resource)
                        resource_id_map[resource.resource_hash] = resource_id
                        resources_created += 1

                    # Track datasource IDs for metric definitions
                    datasource_id_map = {}  # (name, version) -> datasource_id

                    # Upsert datasources
                    self.logger.debug(f"Upserting {len(unique_datasources)} datasources")
                    for datasource in unique_datasources:
                        datasource_id = await self.upsert_datasource(conn, datasource)
                        datasource_id_map[(datasource.name, datasource.version)] = datasource_id
                        datasources_created += 1

                    # Track metric definition IDs for data point insertion
                    metric_def_id_map = {}  # (datasource_name, datasource_version, metric_name) -> metric_def_id

                    # Upsert metric definitions
                    self.logger.debug(f"Upserting {len(unique_metric_defs)} metric definitions")
                    for metric_def in unique_metric_defs:
                        datasource_id = datasource_id_map.get(
                            (metric_def.datasource_name, metric_def.datasource_version)
                        )

                        if not datasource_id:
                            self.logger.warning(
                                f"Datasource not found for metric: {metric_def.datasource_name} v{metric_def.datasource_version}"
                            )
                            continue

                        metric_def_id = await self.upsert_metric_definition(conn, metric_def, datasource_id)
                        metric_def_id_map[
                            (metric_def.datasource_name, metric_def.datasource_version, metric_def.name)
                        ] = metric_def_id
                        metric_defs_created += 1

                    # Insert metric data points
                    self.logger.debug(f"Inserting {len(parsed.metric_data)} metric data points")
                    for data_point in parsed.metric_data:
                        # Get resource ID
                        resource_id = resource_id_map.get(data_point.resource_hash)
                        if not resource_id:
                            self.logger.warning(f"Resource not found for data point: {data_point.resource_hash}")
                            continue

                        # Get metric definition ID
                        metric_def_id = metric_def_id_map.get(
                            (data_point.datasource_name, data_point.datasource_version, data_point.metric_name)
                        )
                        if not metric_def_id:
                            self.logger.warning(
                                f"Metric definition not found: {data_point.datasource_name}/{data_point.metric_name}"
                            )
                            continue

                        await self.insert_metric_data_point(conn, data_point, resource_id, metric_def_id)
                        metric_data_created += 1

                    # Mark as success
                    await self.mark_as_success(conn, lm_metrics_id, metric_data_created)

                # Transaction committed automatically if no exception

                self.logger.info(
                    f"Successfully processed record {lm_metrics_id}: "
                    f"{resources_created} resources, {datasources_created} datasources, "
                    f"{metric_defs_created} metric defs, {metric_data_created} data points"
                )

                return ProcessingResult(
                    lm_metrics_id=lm_metrics_id,
                    success=True,
                    resources_created=resources_created,
                    datasources_created=datasources_created,
                    metric_definitions_created=metric_defs_created,
                    metric_data_created=metric_data_created
                )

            except Exception as e:
                # Transaction automatically rolled back on exception
                error_msg = str(e)
                self.logger.error(f"Failed to process record {lm_metrics_id}: {error_msg}", exc_info=True)

                # Mark as failed (in a new transaction)
                try:
                    async with conn.transaction():
                        await self.mark_as_failed(conn, lm_metrics_id, error_msg)
                except Exception as mark_error:
                    self.logger.error(f"Failed to mark record as failed: {mark_error}")

                return ProcessingResult(
                    lm_metrics_id=lm_metrics_id,
                    success=False,
                    resources_created=0,
                    datasources_created=0,
                    metric_definitions_created=0,
                    metric_data_created=0,
                    error_message=error_msg
                )

    async def process_batch(
        self,
        limit: Optional[int] = None,
        continue_on_error: bool = True
    ) -> BatchProcessingStats:
        """
        Process a batch of unprocessed records.

        Args:
            limit: Maximum number of records to process (None for all)
            continue_on_error: If True, continue processing even if some records fail

        Returns:
            BatchProcessingStats with overall statistics
        """
        self.logger.info(f"Starting batch processing (limit={limit})")

        # Get unprocessed records
        records = await self.get_unprocessed_records(limit)
        total = len(records)

        self.logger.info(f"Found {total} unprocessed records")

        if total == 0:
            return BatchProcessingStats(
                total_records=0,
                successful=0,
                failed=0,
                resources_created=0,
                datasources_created=0,
                metric_definitions_created=0,
                metric_data_created=0,
                errors=[]
            )

        successful = 0
        failed = 0
        total_resources = 0
        total_datasources = 0
        total_metric_defs = 0
        total_metric_data = 0
        errors = []

        for i, (record_id, payload) in enumerate(records, 1):
            self.logger.info(f"Processing record {i}/{total}: ID={record_id}")

            result = await self.process_single_record(record_id, payload)

            if result.success:
                successful += 1
                total_resources += result.resources_created
                total_datasources += result.datasources_created
                total_metric_defs += result.metric_definitions_created
                total_metric_data += result.metric_data_created
            else:
                failed += 1
                if result.error_message:
                    errors.append(f"Record {record_id}: {result.error_message}")

                if not continue_on_error:
                    self.logger.error(f"Stopping batch processing due to error in record {record_id}")
                    break

        stats = BatchProcessingStats(
            total_records=total,
            successful=successful,
            failed=failed,
            resources_created=total_resources,
            datasources_created=total_datasources,
            metric_definitions_created=total_metric_defs,
            metric_data_created=total_metric_data,
            errors=errors
        )

        self.logger.info(
            f"Batch processing complete: {successful}/{total} successful, "
            f"{failed} failed, {total_metric_data} data points created"
        )

        return stats
