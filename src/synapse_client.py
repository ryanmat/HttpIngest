# Description: Azure Synapse Serverless SQL client for querying Data Lake Parquet files
# Description: Provides ML training data queries against historical metric data

"""
Synapse Serverless SQL Client.

Queries Azure Data Lake Gen2 Parquet files through Synapse Serverless SQL.
Used for ML training data requests that span beyond the hot cache window (48h).

Cost: ~$5 per TB scanned (pay-per-query model)
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pyodbc

logger = logging.getLogger(__name__)


class SynapseConfig:
    """Configuration for Synapse Serverless connection."""

    def __init__(
        self,
        server: str,
        database: str = "master",
        datalake_account: str = "stlmingestdatalake",
        datalake_filesystem: str = "metrics",
        datalake_base_path: str = "otlp",
    ):
        self.server = server
        self.database = database
        self.datalake_account = datalake_account
        self.datalake_filesystem = datalake_filesystem
        self.datalake_base_path = datalake_base_path

    @classmethod
    def from_env(cls) -> "SynapseConfig":
        """Create config from environment variables."""
        return cls(
            server=os.getenv("SYNAPSE_SERVER", "syn-lm-analytics-ondemand.sql.azuresynapse.net"),
            database=os.getenv("SYNAPSE_DATABASE", "master"),
            datalake_account=os.getenv("DATALAKE_ACCOUNT", "stlmingestdatalake"),
            datalake_filesystem=os.getenv("DATALAKE_FILESYSTEM", "metrics"),
            datalake_base_path=os.getenv("DATALAKE_BASE_PATH", "otlp"),
        )

    @property
    def metric_data_path(self) -> str:
        """Get the full ABFSS path for metric data with wildcard for partitioned files.

        Uses Hive-style partition pattern: year=*/month=*/day=*/hour=*/*.parquet
        Synapse OPENROWSET requires single wildcards at each directory level.
        """
        return (
            f"abfss://{self.datalake_filesystem}@{self.datalake_account}.dfs.core.windows.net/"
            f"{self.datalake_base_path}/metric_data/year=*/month=*/day=*/hour=*/*.parquet"
        )

    @property
    def resources_path(self) -> str:
        """Get the full ABFSS path for resources with wildcard for Parquet files."""
        return (
            f"abfss://{self.datalake_filesystem}@{self.datalake_account}.dfs.core.windows.net/"
            f"{self.datalake_base_path}/resources/*.parquet"
        )


class SynapseClient:
    """
    Client for querying Synapse Serverless SQL.

    Uses Azure AD authentication (managed identity in Azure, DefaultAzureCredential locally).
    Queries Parquet files directly from Data Lake Gen2.
    """

    def __init__(self, config: SynapseConfig):
        self.config = config
        self._connection: Optional[pyodbc.Connection] = None

    def _get_connection(self, force_reconnect: bool = False) -> pyodbc.Connection:
        """Get or create a Synapse connection using Azure AD auth.

        Synapse Serverless drops idle connections after ~5 minutes. The pyodbc
        'closed' property does not detect dead TCP connections, so we validate
        the cached connection with a lightweight query before returning it.
        On failure (or when force_reconnect=True), we create a fresh connection.
        """
        if not force_reconnect and self._connection is not None and not self._connection.closed:
            # Validate the cached connection is still alive
            try:
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return self._connection
            except pyodbc.Error:
                logger.warning("Cached Synapse connection is stale, reconnecting")
                try:
                    self._connection.close()
                except Exception:
                    pass
                self._connection = None

        # Create a fresh connection
        use_managed_identity = os.getenv("USE_MANAGED_IDENTITY", "false").lower() == "true"

        if use_managed_identity:
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"Authentication=ActiveDirectoryMsi;"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
            )
            try:
                self._connection = pyodbc.connect(conn_str, timeout=30)
                logger.info(f"Connected to Synapse: {self.config.server}")
            except pyodbc.Error as e:
                logger.error(f"Failed to connect to Synapse: {e}")
                raise
        else:
            from azure.identity import DefaultAzureCredential
            import struct

            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/.default")
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
            )
            token_bytes = token.token.encode("utf-16-le")
            token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

            try:
                self._connection = pyodbc.connect(
                    conn_str,
                    timeout=30,
                    attrs_before={1256: token_struct}  # SQL_COPT_SS_ACCESS_TOKEN
                )
                logger.info(f"Connected to Synapse with token: {self.config.server}")
            except pyodbc.Error as e:
                logger.error(f"Failed to connect to Synapse with token: {e}")
                raise

        return self._connection

    def close(self):
        """Close the connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None

    async def get_training_data(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_names: Optional[List[str]] = None,
        resource_hash: Optional[str] = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Query training data from Synapse (Data Lake Parquet files).

        Args:
            start_time: Start of time range
            end_time: End of time range
            metric_names: Optional list of metric names to filter
            resource_hash: Optional resource hash to filter
            limit: Maximum rows to return
            offset: Pagination offset

        Returns:
            Dict with data rows and metadata
        """
        return await self._execute_training_query(
            start_time, end_time, metric_names, resource_hash, limit, offset
        )

    async def _execute_training_query(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_names: Optional[List[str]],
        resource_hash: Optional[str],
        limit: int,
        offset: int,
        _retried: bool = False,
    ) -> Dict[str, Any]:
        """Execute training data query with one retry on stale connections."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Build partition filter for efficient queries
        # This uses the year/month/day/hour partitioning in the Parquet files
        partition_filter = self._build_partition_filter(start_time, end_time)

        # Build the OPENROWSET query for Parquet files
        # Read value_double as VARCHAR to handle NaN/Infinity in legacy files,
        # then convert to float with TRY_CONVERT (returns NULL for invalid values)
        query = f"""
            SELECT
                resource_hash,
                datasource_name,
                metric_name,
                timestamp,
                TRY_CONVERT(float, value_double) as value_double,
                value_int,
                attributes,
                ingested_at
            FROM OPENROWSET(
                BULK '{self.config.metric_data_path}',
                FORMAT = 'PARQUET'
            ) WITH (
                resource_hash VARCHAR(100),
                datasource_name VARCHAR(500),
                metric_name VARCHAR(500),
                timestamp DATETIME2,
                value_double VARCHAR(50),
                value_int BIGINT,
                attributes VARCHAR(MAX),
                ingested_at DATETIME2,
                year SMALLINT,
                month TINYINT,
                day TINYINT,
                hour TINYINT
            ) AS [data]
            WHERE timestamp >= ? AND timestamp <= ?
            {partition_filter}
        """
        params = [start_time, end_time]

        if metric_names:
            placeholders = ",".join(["?" for _ in metric_names])
            query += f" AND metric_name IN ({placeholders})"
            params.extend(metric_names)

        if resource_hash:
            query += " AND resource_hash = ?"
            params.append(resource_hash)

        query += f" ORDER BY timestamp OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

        try:
            # Convert datetime to ISO format strings for Synapse compatibility
            str_params = []
            for p in params:
                if isinstance(p, datetime):
                    str_params.append(p.strftime('%Y-%m-%dT%H:%M:%S'))
                else:
                    str_params.append(p)

            logger.info(f"Executing Synapse training data query with params: {str_params[:2]}")
            cursor.execute(query, str_params)

            # Check if we got a result set
            if cursor.description is None:
                logger.warning("Synapse query returned no result set - may be no matching data")
                return {
                    "data": [],
                    "meta": {
                        "total": 0,
                        "limit": limit,
                        "offset": offset,
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "source": "synapse_datalake",
                        "warning": "No matching data found in specified time range",
                    },
                }

            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Convert timestamps to ISO format
                if row_dict.get("timestamp"):
                    row_dict["timestamp"] = row_dict["timestamp"].isoformat()
                if row_dict.get("ingested_at"):
                    row_dict["ingested_at"] = row_dict["ingested_at"].isoformat()
                # Use value_double or value_int
                row_dict["value"] = row_dict.get("value_double") or row_dict.get("value_int")
                data.append(row_dict)

            # Get total count (separate query for pagination)
            # Use same schema to avoid NaN errors in legacy files
            count_query = f"""
                SELECT COUNT(*) as total
                FROM OPENROWSET(
                    BULK '{self.config.metric_data_path}',
                    FORMAT = 'PARQUET'
                ) WITH (
                    timestamp DATETIME2,
                    metric_name VARCHAR(500),
                    resource_hash VARCHAR(100),
                    year SMALLINT,
                    month TINYINT
                ) AS [data]
                WHERE timestamp >= ? AND timestamp <= ?
                {partition_filter}
            """
            count_params = [start_time.strftime('%Y-%m-%dT%H:%M:%S'), end_time.strftime('%Y-%m-%dT%H:%M:%S')]
            if metric_names:
                placeholders = ",".join(["?" for _ in metric_names])
                count_query += f" AND metric_name IN ({placeholders})"
                count_params.extend(metric_names)
            if resource_hash:
                count_query += " AND resource_hash = ?"
                count_params.append(resource_hash)

            cursor.execute(count_query, count_params)
            count_row = cursor.fetchone()
            total = count_row[0] if count_row else 0

            return {
                "data": data,
                "meta": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "source": "synapse_datalake",
                },
            }

        except pyodbc.Error as e:
            # Retry once with a fresh connection on communication link failures
            if not _retried and "08S01" in str(e):
                logger.warning(f"Synapse connection lost, retrying with fresh connection: {e}")
                self._connection = None
                return await self._execute_training_query(
                    start_time, end_time, metric_names, resource_hash,
                    limit, offset, _retried=True,
                )
            logger.error(f"Synapse query error: {e}")
            raise

    async def get_inventory(self, _retried: bool = False) -> Dict[str, Any]:
        """
        Get inventory of data available in Data Lake.

        Returns summary of metrics, resources, and time ranges.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Get unique metrics
            metrics_query = f"""
                SELECT DISTINCT
                    metric_name,
                    datasource_name,
                    COUNT(*) as data_points
                FROM OPENROWSET(
                    BULK '{self.config.metric_data_path}',
                    FORMAT = 'PARQUET'
                ) AS [data]
                GROUP BY metric_name, datasource_name
                ORDER BY data_points DESC
            """
            cursor.execute(metrics_query)
            metrics = [
                {"metric_name": row[0], "datasource_name": row[1], "data_points": row[2]}
                for row in cursor.fetchall()
            ]

            # Get unique resources
            resources_query = f"""
                SELECT DISTINCT
                    resource_hash,
                    COUNT(*) as data_points
                FROM OPENROWSET(
                    BULK '{self.config.metric_data_path}',
                    FORMAT = 'PARQUET'
                ) AS [data]
                GROUP BY resource_hash
                ORDER BY data_points DESC
            """
            cursor.execute(resources_query)
            resources = [
                {"resource_hash": row[0], "data_points": row[1]}
                for row in cursor.fetchall()
            ]

            # Get time range
            time_query = f"""
                SELECT
                    MIN(timestamp) as min_ts,
                    MAX(timestamp) as max_ts,
                    COUNT(*) as total
                FROM OPENROWSET(
                    BULK '{self.config.metric_data_path}',
                    FORMAT = 'PARQUET'
                ) AS [data]
            """
            cursor.execute(time_query)
            time_row = cursor.fetchone()

            return {
                "metrics": metrics[:100],  # Top 100
                "resources": resources[:100],  # Top 100
                "time_range": {
                    "start": time_row[0].isoformat() if time_row[0] else None,
                    "end": time_row[1].isoformat() if time_row[1] else None,
                },
                "total_data_points": time_row[2] or 0,
                "source": "synapse_datalake",
            }

        except pyodbc.Error as e:
            if not _retried and "08S01" in str(e):
                logger.warning(f"Synapse connection lost during inventory, retrying: {e}")
                self._connection = None
                return await self.get_inventory(_retried=True)
            logger.error(f"Synapse inventory query error: {e}")
            raise

    def _build_partition_filter(self, start_time: datetime, end_time: datetime) -> str:
        """
        Build partition pruning filter for efficient Parquet queries.

        The Data Lake is partitioned by year/month/day/hour, so we can
        skip scanning partitions outside our time range.
        """
        # Generate partition filter to limit scanned data
        filters = []

        # Year filter
        start_year = start_time.year
        end_year = end_time.year
        if start_year == end_year:
            filters.append(f"AND year = {start_year}")
        else:
            filters.append(f"AND year >= {start_year} AND year <= {end_year}")

        # Month filter (only if same year)
        if start_year == end_year:
            start_month = start_time.month
            end_month = end_time.month
            if start_month == end_month:
                filters.append(f"AND month = {start_month}")
            else:
                filters.append(f"AND month >= {start_month} AND month <= {end_month}")

        return " ".join(filters)

    async def check_health(self) -> Dict[str, Any]:
        """Check Synapse connection health."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return {
                "status": "healthy",
                "server": self.config.server,
                "database": self.config.database,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "server": self.config.server,
            }
