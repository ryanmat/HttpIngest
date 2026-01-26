# Description: Azure Data Lake Gen2 writer for Parquet metric data
# Description: Buffers and writes OTLP data as partitioned Parquet files

"""
Data Lake Writer for Azure Data Lake Gen2.

Buffers incoming metric data and periodically flushes to Parquet files
organized by time partitions (year/month/day/hour).
"""

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from src.otlp_parser import (
    DatasourceData,
    MetricDataPoint,
    MetricDefinitionData,
    ParsedOTLP,
    ResourceData,
)

logger = logging.getLogger(__name__)


# Parquet schema for metric data
METRIC_DATA_SCHEMA = pa.schema([
    pa.field('resource_hash', pa.string()),
    pa.field('datasource_name', pa.string()),
    pa.field('metric_name', pa.string()),
    pa.field('timestamp', pa.timestamp('us', tz='UTC')),
    pa.field('value_double', pa.float64()),
    pa.field('value_int', pa.int64()),
    pa.field('attributes', pa.string()),
    pa.field('ingested_at', pa.timestamp('us', tz='UTC')),
    pa.field('year', pa.int16()),
    pa.field('month', pa.int8()),
    pa.field('day', pa.int8()),
    pa.field('hour', pa.int8()),
])

RESOURCES_SCHEMA = pa.schema([
    pa.field('resource_hash', pa.string()),
    pa.field('attributes', pa.string()),
    pa.field('created_at', pa.timestamp('us', tz='UTC')),
    pa.field('updated_at', pa.timestamp('us', tz='UTC')),
])

DATASOURCES_SCHEMA = pa.schema([
    pa.field('name', pa.string()),
    pa.field('version', pa.string()),
    pa.field('created_at', pa.timestamp('us', tz='UTC')),
])

METRIC_DEFINITIONS_SCHEMA = pa.schema([
    pa.field('datasource_name', pa.string()),
    pa.field('datasource_version', pa.string()),
    pa.field('name', pa.string()),
    pa.field('unit', pa.string()),
    pa.field('metric_type', pa.string()),
    pa.field('description', pa.string()),
    pa.field('created_at', pa.timestamp('us', tz='UTC')),
])


@dataclass
class DataLakeConfig:
    """Configuration for Data Lake connection."""
    account_name: str
    filesystem: str
    base_path: str = "otlp"
    flush_interval_seconds: int = 60
    flush_threshold_rows: int = 10000

    @classmethod
    def from_env(cls) -> "DataLakeConfig":
        """Create config from environment variables."""
        return cls(
            account_name=os.getenv("DATALAKE_ACCOUNT", "stlmingestdatalake"),
            filesystem=os.getenv("DATALAKE_FILESYSTEM", "metrics"),
            base_path=os.getenv("DATALAKE_BASE_PATH", "otlp"),
            flush_interval_seconds=int(os.getenv("DATALAKE_FLUSH_INTERVAL_SECONDS", "60")),
            flush_threshold_rows=int(os.getenv("DATALAKE_FLUSH_THRESHOLD_ROWS", "10000")),
        )


class DataLakeWriter:
    """
    Writes metric data to Azure Data Lake Gen2 as Parquet files.

    Buffers incoming data and flushes periodically or when threshold is reached.
    Files are partitioned by year/month/day/hour for efficient time-range queries.
    """

    def __init__(self, config: DataLakeConfig):
        self.config = config
        self.metric_buffer: List[Dict[str, Any]] = []
        self.resource_buffer: Dict[str, Dict[str, Any]] = {}
        self.datasource_buffer: Dict[str, Dict[str, Any]] = {}
        self.metric_def_buffer: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self._service_client: Optional[DataLakeServiceClient] = None
        self._credential: Optional[DefaultAzureCredential] = None

    def _get_service_client(self) -> DataLakeServiceClient:
        """Get or create the Data Lake service client."""
        if self._service_client is None:
            self._credential = DefaultAzureCredential()
            self._service_client = DataLakeServiceClient(
                account_url=f"https://{self.config.account_name}.dfs.core.windows.net",
                credential=self._credential
            )
        return self._service_client

    async def write_metrics(self, parsed: ParsedOTLP) -> int:
        """
        Buffer metric data for batch writing.

        Args:
            parsed: ParsedOTLP containing resources, datasources, definitions, and data

        Returns:
            Number of metric data points buffered
        """
        async with self.lock:
            now = datetime.now(timezone.utc)

            # Buffer resources (deduplicate by hash)
            for resource in parsed.resources:
                if resource.resource_hash not in self.resource_buffer:
                    self.resource_buffer[resource.resource_hash] = {
                        'resource_hash': resource.resource_hash,
                        'attributes': json.dumps(resource.attributes),
                        'created_at': now,
                        'updated_at': now,
                    }

            # Buffer datasources (deduplicate by name+version)
            for ds in parsed.datasources:
                key = f"{ds.name}|{ds.version or ''}"
                if key not in self.datasource_buffer:
                    self.datasource_buffer[key] = {
                        'name': ds.name,
                        'version': ds.version,
                        'created_at': now,
                    }

            # Buffer metric definitions (deduplicate by datasource+name)
            for md in parsed.metric_definitions:
                key = f"{md.datasource_name}|{md.datasource_version or ''}|{md.name}"
                if key not in self.metric_def_buffer:
                    self.metric_def_buffer[key] = {
                        'datasource_name': md.datasource_name,
                        'datasource_version': md.datasource_version,
                        'name': md.name,
                        'unit': md.unit,
                        'metric_type': md.metric_type,
                        'description': md.description,
                        'created_at': now,
                    }

            # Buffer metric data points
            for dp in parsed.metric_data:
                self.metric_buffer.append(self._datapoint_to_dict(dp, now))

            buffered_count = len(parsed.metric_data)

            # Check if we should flush
            if len(self.metric_buffer) >= self.config.flush_threshold_rows:
                await self._flush_buffer()

        return buffered_count

    async def flush(self) -> int:
        """Force flush all buffers to Data Lake."""
        async with self.lock:
            return await self._flush_buffer()

    async def _flush_buffer(self) -> int:
        """Write buffered data to Parquet files."""
        written = 0

        try:
            service_client = self._get_service_client()
            fs_client = service_client.get_file_system_client(self.config.filesystem)

            # Flush metric data (partitioned by time)
            if self.metric_buffer:
                written = await self._flush_metric_data(fs_client)

            # Flush reference data (append to single files)
            if self.resource_buffer:
                await self._flush_resources(fs_client)

            if self.datasource_buffer:
                await self._flush_datasources(fs_client)

            if self.metric_def_buffer:
                await self._flush_metric_definitions(fs_client)

        except Exception as e:
            logger.error(f"Failed to flush to Data Lake: {e}", exc_info=True)
            raise

        return written

    async def _flush_metric_data(self, fs_client) -> int:
        """Flush metric data buffer, partitioned by time."""
        if not self.metric_buffer:
            return 0

        # Group by partition (year/month/day/hour)
        partitions: Dict[str, List[Dict]] = {}
        for row in self.metric_buffer:
            ts = row['timestamp']
            # Handle both datetime objects and timestamps
            if isinstance(ts, datetime):
                key = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"
            else:
                # Assume it's already a valid timestamp
                dt = ts if isinstance(ts, datetime) else datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
                key = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"
            partitions.setdefault(key, []).append(row)

        written = 0
        for partition_path, rows in partitions.items():
            # Convert to PyArrow table
            table = pa.Table.from_pylist(rows, schema=METRIC_DATA_SCHEMA)

            # Write to buffer
            buffer = BytesIO()
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)

            # Generate unique filename
            filename = f"part-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}.parquet"
            full_path = f"{self.config.base_path}/metric_data/{partition_path}/{filename}"

            # Upload to Data Lake
            file_client = fs_client.get_file_client(full_path)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

            written += len(rows)
            logger.info(f"Wrote {len(rows)} metric records to {full_path}")

        self.metric_buffer.clear()
        return written

    async def _flush_resources(self, fs_client) -> None:
        """Flush resources buffer."""
        if not self.resource_buffer:
            return

        rows = list(self.resource_buffer.values())
        table = pa.Table.from_pylist(rows, schema=RESOURCES_SCHEMA)

        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        filename = f"resources-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}.parquet"
        full_path = f"{self.config.base_path}/resources/{filename}"

        file_client = fs_client.get_file_client(full_path)
        file_client.upload_data(buffer.getvalue(), overwrite=True)

        logger.info(f"Wrote {len(rows)} resources to {full_path}")
        self.resource_buffer.clear()

    async def _flush_datasources(self, fs_client) -> None:
        """Flush datasources buffer."""
        if not self.datasource_buffer:
            return

        rows = list(self.datasource_buffer.values())
        table = pa.Table.from_pylist(rows, schema=DATASOURCES_SCHEMA)

        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        filename = f"datasources-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}.parquet"
        full_path = f"{self.config.base_path}/datasources/{filename}"

        file_client = fs_client.get_file_client(full_path)
        file_client.upload_data(buffer.getvalue(), overwrite=True)

        logger.info(f"Wrote {len(rows)} datasources to {full_path}")
        self.datasource_buffer.clear()

    async def _flush_metric_definitions(self, fs_client) -> None:
        """Flush metric definitions buffer."""
        if not self.metric_def_buffer:
            return

        rows = list(self.metric_def_buffer.values())
        table = pa.Table.from_pylist(rows, schema=METRIC_DEFINITIONS_SCHEMA)

        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        filename = f"metric_definitions-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}.parquet"
        full_path = f"{self.config.base_path}/metric_definitions/{filename}"

        file_client = fs_client.get_file_client(full_path)
        file_client.upload_data(buffer.getvalue(), overwrite=True)

        logger.info(f"Wrote {len(rows)} metric definitions to {full_path}")
        self.metric_def_buffer.clear()

    def _datapoint_to_dict(self, dp: MetricDataPoint, ingested_at: datetime) -> Dict[str, Any]:
        """Convert MetricDataPoint to dictionary for Parquet."""
        ts = dp.timestamp
        return {
            'resource_hash': dp.resource_hash,
            'datasource_name': dp.datasource_name,
            'metric_name': dp.metric_name,
            'timestamp': ts,
            'value_double': dp.value_double,
            'value_int': dp.value_int,
            'attributes': json.dumps(dp.attributes) if dp.attributes else None,
            'ingested_at': ingested_at,
            'year': ts.year,
            'month': ts.month,
            'day': ts.day,
            'hour': ts.hour,
        }

    def get_buffer_stats(self) -> Dict[str, int]:
        """Get current buffer statistics."""
        return {
            'metric_data_buffered': len(self.metric_buffer),
            'resources_buffered': len(self.resource_buffer),
            'datasources_buffered': len(self.datasource_buffer),
            'metric_definitions_buffered': len(self.metric_def_buffer),
            'flush_threshold': self.config.flush_threshold_rows,
        }
