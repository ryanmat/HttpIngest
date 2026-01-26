# Description: Unit tests for Data Lake migration components
# Description: Tests datalake_writer, synapse_client, hot_cache_manager, and ingestion_router

"""
Unit tests for the Azure Data Lake Gen2 migration components.

Tests the following modules:
- datalake_writer: Parquet buffering and partitioning
- synapse_client: Configuration and partition filter building
- hot_cache_manager: TTL-based cleanup logic
- ingestion_router: Dual-write routing and statistics
"""

import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.datalake_writer import (
    METRIC_DATA_SCHEMA,
    DataLakeConfig,
    DataLakeWriter,
)
from src.hot_cache_manager import HotCacheManager
from src.ingestion_router import IngestionConfig, IngestionRouter, IngestionStats
from src.otlp_parser import (
    DatasourceData,
    MetricDataPoint,
    MetricDefinitionData,
    ParsedOTLP,
    ResourceData,
)

# Synapse client requires pyodbc which needs ODBC drivers - make import optional
try:
    from src.synapse_client import SynapseClient, SynapseConfig
    SYNAPSE_AVAILABLE = True
except ImportError:
    SYNAPSE_AVAILABLE = False
    SynapseClient = None
    SynapseConfig = None


class TestDataLakeConfig:
    """Tests for DataLakeConfig."""

    def test_from_env_defaults(self):
        """Test DataLakeConfig uses defaults when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = DataLakeConfig.from_env()

            assert config.account_name == "stlmingestdatalake"
            assert config.filesystem == "metrics"
            assert config.base_path == "otlp"
            assert config.flush_interval_seconds == 60
            assert config.flush_threshold_rows == 10000

    def test_from_env_custom(self):
        """Test DataLakeConfig uses custom env vars."""
        custom_env = {
            "DATALAKE_ACCOUNT": "customaccount",
            "DATALAKE_FILESYSTEM": "customfs",
            "DATALAKE_BASE_PATH": "custom/path",
            "DATALAKE_FLUSH_INTERVAL_SECONDS": "120",
            "DATALAKE_FLUSH_THRESHOLD_ROWS": "5000",
        }
        with patch.dict(os.environ, custom_env, clear=True):
            config = DataLakeConfig.from_env()

            assert config.account_name == "customaccount"
            assert config.filesystem == "customfs"
            assert config.base_path == "custom/path"
            assert config.flush_interval_seconds == 120
            assert config.flush_threshold_rows == 5000


class TestDataLakeWriter:
    """Tests for DataLakeWriter."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        return DataLakeConfig(
            account_name="testaccount",
            filesystem="testfs",
            base_path="test",
            flush_threshold_rows=100,
        )

    @pytest.fixture
    def writer(self, config):
        """Create test writer."""
        return DataLakeWriter(config)

    def test_datapoint_to_dict(self, writer):
        """Test converting MetricDataPoint to dict."""
        now = datetime.now(timezone.utc)
        dp = MetricDataPoint(
            resource_hash="abc123",
            datasource_name="test-ds",
            datasource_version="1.0",
            metric_name="cpu.usage",
            timestamp=now,
            value_double=42.5,
            value_int=None,
            attributes={"host": "server1"},
        )

        result = writer._datapoint_to_dict(dp, now)

        assert result["resource_hash"] == "abc123"
        assert result["datasource_name"] == "test-ds"
        assert result["metric_name"] == "cpu.usage"
        assert result["timestamp"] == now
        assert result["value_double"] == 42.5
        assert result["value_int"] is None
        assert result["attributes"] == '{"host": "server1"}'
        assert result["year"] == now.year
        assert result["month"] == now.month
        assert result["day"] == now.day
        assert result["hour"] == now.hour

    def test_datapoint_to_dict_no_attributes(self, writer):
        """Test converting MetricDataPoint with no attributes."""
        now = datetime.now(timezone.utc)
        dp = MetricDataPoint(
            resource_hash="abc123",
            datasource_name="test-ds",
            datasource_version="1.0",
            metric_name="cpu.usage",
            timestamp=now,
            value_double=None,
            value_int=100,
            attributes=None,
        )

        result = writer._datapoint_to_dict(dp, now)

        assert result["value_int"] == 100
        assert result["attributes"] is None

    def test_get_buffer_stats_empty(self, writer):
        """Test buffer stats when empty."""
        stats = writer.get_buffer_stats()

        assert stats["metric_data_buffered"] == 0
        assert stats["resources_buffered"] == 0
        assert stats["datasources_buffered"] == 0
        assert stats["metric_definitions_buffered"] == 0
        assert stats["flush_threshold"] == 100

    @pytest.mark.asyncio
    async def test_write_metrics_buffers_data(self, writer):
        """Test that write_metrics buffers data correctly."""
        now = datetime.now(timezone.utc)
        parsed = ParsedOTLP(
            resources=[
                ResourceData(resource_hash="res1", attributes={"service": "test"})
            ],
            datasources=[
                DatasourceData(name="ds1", version="1.0")
            ],
            metric_definitions=[
                MetricDefinitionData(
                    datasource_name="ds1",
                    datasource_version="1.0",
                    name="metric1",
                    unit="1",
                    metric_type="gauge",
                    description="Test metric",
                )
            ],
            metric_data=[
                MetricDataPoint(
                    resource_hash="res1",
                    datasource_name="ds1",
                    datasource_version="1.0",
                    metric_name="metric1",
                    timestamp=now,
                    value_double=42.0,
                    value_int=None,
                    attributes=None,
                )
            ],
        )

        count = await writer.write_metrics(parsed)

        assert count == 1
        stats = writer.get_buffer_stats()
        assert stats["metric_data_buffered"] == 1
        assert stats["resources_buffered"] == 1
        assert stats["datasources_buffered"] == 1
        assert stats["metric_definitions_buffered"] == 1

    @pytest.mark.asyncio
    async def test_write_metrics_deduplicates_resources(self, writer):
        """Test that duplicate resources are deduplicated."""
        now = datetime.now(timezone.utc)

        # Add same resource twice
        for _ in range(2):
            parsed = ParsedOTLP(
                resources=[
                    ResourceData(resource_hash="same-hash", attributes={"service": "test"})
                ],
                datasources=[],
                metric_definitions=[],
                metric_data=[],
            )
            await writer.write_metrics(parsed)

        stats = writer.get_buffer_stats()
        assert stats["resources_buffered"] == 1  # Only one resource

    def test_metric_data_schema_fields(self):
        """Test that METRIC_DATA_SCHEMA has expected fields."""
        field_names = [f.name for f in METRIC_DATA_SCHEMA]

        assert "resource_hash" in field_names
        assert "datasource_name" in field_names
        assert "metric_name" in field_names
        assert "timestamp" in field_names
        assert "value_double" in field_names
        assert "value_int" in field_names
        assert "attributes" in field_names
        assert "ingested_at" in field_names
        assert "year" in field_names
        assert "month" in field_names
        assert "day" in field_names
        assert "hour" in field_names


@pytest.mark.skipif(not SYNAPSE_AVAILABLE, reason="pyodbc/ODBC drivers not available")
class TestSynapseConfig:
    """Tests for SynapseConfig."""

    def test_from_env_defaults(self):
        """Test SynapseConfig uses defaults when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = SynapseConfig.from_env()

            assert "sql.azuresynapse.net" in config.server
            assert config.database == "master"
            assert config.datalake_account == "stlmingestdatalake"
            assert config.datalake_filesystem == "metrics"
            assert config.datalake_base_path == "otlp"

    def test_metric_data_path(self):
        """Test metric_data_path property."""
        config = SynapseConfig(
            server="test.sql.azuresynapse.net",
            datalake_account="testaccount",
            datalake_filesystem="testfs",
            datalake_base_path="base",
        )

        path = config.metric_data_path

        assert "abfss://testfs@testaccount.dfs.core.windows.net" in path
        assert "base/metric_data/" in path

    def test_resources_path(self):
        """Test resources_path property."""
        config = SynapseConfig(
            server="test.sql.azuresynapse.net",
            datalake_account="testaccount",
            datalake_filesystem="testfs",
            datalake_base_path="base",
        )

        path = config.resources_path

        assert "abfss://testfs@testaccount.dfs.core.windows.net" in path
        assert "base/resources/" in path


@pytest.mark.skipif(not SYNAPSE_AVAILABLE, reason="pyodbc/ODBC drivers not available")
class TestSynapseClient:
    """Tests for SynapseClient."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        return SynapseConfig(
            server="test.sql.azuresynapse.net",
            database="testdb",
        )

    @pytest.fixture
    def client(self, config):
        """Create test client."""
        return SynapseClient(config)

    def test_build_partition_filter_same_year(self, client):
        """Test partition filter for same year range."""
        start = datetime(2024, 3, 15, 10, 0, 0)
        end = datetime(2024, 3, 15, 12, 0, 0)

        filter_str = client._build_partition_filter(start, end)

        assert "year = 2024" in filter_str
        assert "month = 3" in filter_str

    def test_build_partition_filter_different_years(self, client):
        """Test partition filter spanning different years."""
        start = datetime(2023, 11, 1, 0, 0, 0)
        end = datetime(2024, 2, 1, 0, 0, 0)

        filter_str = client._build_partition_filter(start, end)

        assert "year >= 2023" in filter_str
        assert "year <= 2024" in filter_str
        # Month filter should not be added for different years
        assert "month" not in filter_str

    def test_build_partition_filter_different_months(self, client):
        """Test partition filter spanning different months same year."""
        start = datetime(2024, 3, 15, 10, 0, 0)
        end = datetime(2024, 5, 20, 12, 0, 0)

        filter_str = client._build_partition_filter(start, end)

        assert "year = 2024" in filter_str
        assert "month >= 3" in filter_str
        assert "month <= 5" in filter_str


class TestHotCacheManager:
    """Tests for HotCacheManager."""

    @pytest.fixture
    def mock_pool(self):
        """Create mock asyncpg pool."""
        pool = MagicMock()
        return pool

    def test_init_default_retention(self, mock_pool):
        """Test default retention hours."""
        with patch.dict(os.environ, {}, clear=True):
            manager = HotCacheManager(mock_pool)
            assert manager.retention_hours == 48

    def test_init_custom_retention(self, mock_pool):
        """Test custom retention hours from env."""
        with patch.dict(os.environ, {"HOT_CACHE_RETENTION_HOURS": "24"}, clear=True):
            manager = HotCacheManager(mock_pool)
            assert manager.retention_hours == 24

    def test_init_explicit_retention(self, mock_pool):
        """Test explicit retention hours parameter."""
        manager = HotCacheManager(mock_pool, retention_hours=12)
        assert manager.retention_hours == 12

    @pytest.mark.asyncio
    async def test_cleanup_expired_data_parses_delete_result(self):
        """Test cleanup correctly parses DELETE results."""
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(
            side_effect=["DELETE 100", "DELETE 50", "DELETE 25"]
        )

        # Create proper async context manager
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=mock_cm)

        manager = HotCacheManager(mock_pool)
        result = await manager.cleanup_expired_data()

        assert result["metric_data"] == 100
        assert result["processing_status"] == 50
        assert result["lm_metrics"] == 25

    @pytest.mark.asyncio
    async def test_is_healthy_returns_true_for_empty_cache(self):
        """Test health check returns true for empty cache."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=None)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=mock_cm)

        manager = HotCacheManager(mock_pool)
        result = await manager.is_healthy()

        assert result is True

    @pytest.mark.asyncio
    async def test_is_healthy_returns_true_for_recent_data(self):
        """Test health check returns true for recent data."""
        recent_time = datetime.now(timezone.utc) - timedelta(hours=1)
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=recent_time)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=mock_cm)

        manager = HotCacheManager(mock_pool, retention_hours=48)
        result = await manager.is_healthy()

        assert result is True

    @pytest.mark.asyncio
    async def test_is_healthy_returns_false_for_stale_data(self):
        """Test health check returns false for stale data."""
        stale_time = datetime.now(timezone.utc) - timedelta(hours=200)
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=stale_time)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=mock_cm)

        manager = HotCacheManager(mock_pool, retention_hours=48)
        result = await manager.is_healthy()

        assert result is False

    @pytest.mark.asyncio
    async def test_is_healthy_returns_false_on_exception(self):
        """Test health check returns false on exception."""
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(side_effect=Exception("Connection error"))

        manager = HotCacheManager(mock_pool)
        result = await manager.is_healthy()

        assert result is False


class TestIngestionConfig:
    """Tests for IngestionConfig."""

    def test_from_env_defaults(self):
        """Test default values."""
        with patch.dict(os.environ, {}, clear=True):
            config = IngestionConfig.from_env()

            assert config.write_to_datalake is True
            assert config.write_to_hot_cache is True

    def test_from_env_disabled(self):
        """Test disabled writes."""
        env = {
            "WRITE_TO_DATALAKE": "false",
            "WRITE_TO_HOT_CACHE": "false",
        }
        with patch.dict(os.environ, env, clear=True):
            config = IngestionConfig.from_env()

            assert config.write_to_datalake is False
            assert config.write_to_hot_cache is False


class TestIngestionStats:
    """Tests for IngestionStats."""

    def test_to_dict(self):
        """Test IngestionStats.to_dict()."""
        stats = IngestionStats(
            resources=5,
            datasources=2,
            metric_definitions=10,
            metric_data=100,
            datalake_written=100,
            hot_cache_written=100,
            errors=[],
        )

        result = stats.to_dict()

        assert result["resources"] == 5
        assert result["datasources"] == 2
        assert result["metric_definitions"] == 10
        assert result["metric_data"] == 100
        assert result["datalake_written"] == 100
        assert result["hot_cache_written"] == 100
        assert result["errors"] == []

    def test_to_dict_with_errors(self):
        """Test IngestionStats.to_dict() with errors."""
        stats = IngestionStats(
            resources=0,
            datasources=0,
            metric_definitions=0,
            metric_data=0,
            datalake_written=0,
            hot_cache_written=0,
            errors=["Error 1", "Error 2"],
        )

        result = stats.to_dict()

        assert result["errors"] == ["Error 1", "Error 2"]


class TestIngestionRouter:
    """Tests for IngestionRouter."""

    @pytest.fixture
    def mock_datalake_writer(self):
        """Create mock DataLakeWriter."""
        writer = AsyncMock(spec=DataLakeWriter)
        writer.write_metrics = AsyncMock(return_value=10)
        return writer

    @pytest.fixture
    def mock_db_pool(self):
        """Create mock db pool."""
        return AsyncMock()

    @pytest.fixture
    def config(self):
        """Create test config."""
        return IngestionConfig(
            write_to_datalake=True,
            write_to_hot_cache=False,  # Disable hot cache for simpler tests
        )

    @pytest.fixture
    def router(self, mock_datalake_writer, mock_db_pool, config):
        """Create test router."""
        return IngestionRouter(
            datalake_writer=mock_datalake_writer,
            db_pool=mock_db_pool,
            config=config,
        )

    @pytest.mark.asyncio
    async def test_ingest_valid_payload(self, router, mock_datalake_writer):
        """Test ingesting valid OTLP payload."""
        payload = {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "test"}}
                        ]
                    },
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test"},
                            "metrics": [
                                {
                                    "name": "test.metric",
                                    "unit": "1",
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "asInt": 42,
                                                "timeUnixNano": str(int(datetime.now().timestamp() * 1e9)),
                                            }
                                        ]
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        stats = await router.ingest(payload)

        assert stats.resources == 1
        assert stats.datasources == 1
        assert stats.metric_definitions == 1
        assert stats.metric_data == 1
        assert stats.datalake_written == 10
        assert stats.errors == []
        mock_datalake_writer.write_metrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_ingest_invalid_payload(self, router):
        """Test ingesting invalid payload returns error."""
        payload = {"invalid": "payload"}

        stats = await router.ingest(payload)

        assert stats.resources == 0
        assert stats.metric_data == 0
        assert len(stats.errors) > 0
        assert "Parse error" in stats.errors[0]

    @pytest.mark.asyncio
    async def test_ingest_handles_datalake_error(self, router, mock_datalake_writer):
        """Test ingestion handles Data Lake write errors."""
        mock_datalake_writer.write_metrics = AsyncMock(
            side_effect=Exception("Upload failed")
        )

        payload = {
            "resourceMetrics": [
                {
                    "resource": {"attributes": []},
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test"},
                            "metrics": [
                                {
                                    "name": "test.metric",
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "asInt": 1,
                                                "timeUnixNano": str(int(datetime.now().timestamp() * 1e9)),
                                            }
                                        ]
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        stats = await router.ingest(payload)

        assert stats.datalake_written == 0
        assert any("Data Lake write error" in e for e in stats.errors)

    @pytest.mark.asyncio
    async def test_get_status(self, router, mock_datalake_writer):
        """Test get_status returns expected structure."""
        mock_datalake_writer.get_buffer_stats = MagicMock(
            return_value={"metric_data_buffered": 5}
        )

        status = await router.get_status()

        assert "config" in status
        assert status["config"]["write_to_datalake"] is True
        assert status["config"]["write_to_hot_cache"] is False
        assert "datalake" in status
        assert status["datalake"]["metric_data_buffered"] == 5

    @pytest.mark.asyncio
    async def test_router_without_datalake_writer(self, mock_db_pool):
        """Test router works without datalake writer."""
        config = IngestionConfig(write_to_datalake=True, write_to_hot_cache=False)
        router = IngestionRouter(
            datalake_writer=None,
            db_pool=mock_db_pool,
            config=config,
        )

        payload = {
            "resourceMetrics": [
                {
                    "resource": {"attributes": []},
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test"},
                            "metrics": [
                                {
                                    "name": "test.metric",
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "asInt": 1,
                                                "timeUnixNano": str(int(datetime.now().timestamp() * 1e9)),
                                            }
                                        ]
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        stats = await router.ingest(payload)

        # Should complete without errors (just no data written)
        assert stats.datalake_written == 0
        assert "Data Lake write error" not in str(stats.errors)


class TestExportersHotCacheValidation:
    """Tests for exporters 48h time limit validation."""

    def test_hot_cache_time_range_error_message(self):
        """Test HotCacheTimeRangeError has informative message."""
        from src.exporters import HotCacheTimeRangeError

        requested = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        allowed = datetime(2024, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        error = HotCacheTimeRangeError(requested, allowed)

        assert "2024-01-01" in str(error)
        assert "2024-01-10" in str(error)
        assert "hot cache window" in str(error)
        assert "Synapse" in str(error)

    def test_validate_hot_cache_passes_when_disabled(self):
        """Test validation passes when hot cache is disabled."""
        from src.exporters import validate_hot_cache_time_range

        with patch("src.exporters.HOT_CACHE_ENABLED", False):
            # Should not raise even for very old times
            old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
            validate_hot_cache_time_range(old_time, datetime.now(timezone.utc))

    def test_validate_hot_cache_passes_for_recent_data(self):
        """Test validation passes for recent time range."""
        from src.exporters import validate_hot_cache_time_range

        with patch("src.exporters.HOT_CACHE_ENABLED", True):
            with patch("src.exporters.HOT_CACHE_RETENTION_HOURS", 48):
                recent = datetime.now(timezone.utc) - timedelta(hours=24)
                validate_hot_cache_time_range(recent, datetime.now(timezone.utc))

    def test_validate_hot_cache_raises_for_old_data(self):
        """Test validation raises for data beyond retention window."""
        from src.exporters import HotCacheTimeRangeError, validate_hot_cache_time_range

        with patch("src.exporters.HOT_CACHE_ENABLED", True):
            with patch("src.exporters.HOT_CACHE_RETENTION_HOURS", 48):
                old_time = datetime.now(timezone.utc) - timedelta(hours=100)

                with pytest.raises(HotCacheTimeRangeError):
                    validate_hot_cache_time_range(old_time, datetime.now(timezone.utc))

    def test_validate_hot_cache_none_start_time_passes(self):
        """Test validation passes when start_time is None."""
        from src.exporters import validate_hot_cache_time_range

        with patch("src.exporters.HOT_CACHE_ENABLED", True):
            validate_hot_cache_time_range(None, datetime.now(timezone.utc))
