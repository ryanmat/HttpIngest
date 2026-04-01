# Description: Unit tests for Data Lake ingestion components
# Description: Tests datalake_writer, ingestion_router config, stats, and routing

"""
Unit tests for the Azure Data Lake Gen2 ingestion components.

Tests the following modules:
- datalake_writer: Parquet buffering and partitioning
- ingestion_router: Routing configuration and statistics
"""

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.datalake_writer import (
    METRIC_DATA_SCHEMA,
    DataLakeConfig,
    DataLakeWriter,
)
from src.ingestion_router import IngestionConfig, IngestionRouter, IngestionStats
from src.otlp_parser import (
    DatasourceData,
    MetricDataPoint,
    MetricDefinitionData,
    ParsedOTLP,
    ResourceData,
)


class TestDataLakeConfig:
    """Tests for DataLakeConfig."""

    def test_from_env_defaults(self):
        """Test DataLakeConfig uses defaults when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = DataLakeConfig.from_env()

            assert config.account_name == "stlmingestdatalake"
            assert config.filesystem == "metrics"
            assert config.base_path == "otlp"
            assert config.flush_interval_seconds == 600
            assert config.flush_threshold_rows == 50000

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

    def test_datapoint_to_dict_sanitizes_nan(self, writer):
        """Test that NaN values are converted to None for Parquet compatibility."""
        now = datetime.now(timezone.utc)
        dp = MetricDataPoint(
            resource_hash="abc123",
            datasource_name="test-ds",
            datasource_version="1.0",
            metric_name="cpu.usage",
            timestamp=now,
            value_double=float("nan"),
            value_int=None,
            attributes=None,
        )

        result = writer._datapoint_to_dict(dp, now)

        assert result["value_double"] is None

    def test_datapoint_to_dict_sanitizes_infinity(self, writer):
        """Test that Infinity values are converted to None for Parquet compatibility."""
        now = datetime.now(timezone.utc)
        dp = MetricDataPoint(
            resource_hash="abc123",
            datasource_name="test-ds",
            datasource_version="1.0",
            metric_name="cpu.usage",
            timestamp=now,
            value_double=float("inf"),
            value_int=None,
            attributes=None,
        )

        result = writer._datapoint_to_dict(dp, now)

        assert result["value_double"] is None

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
            datasources=[DatasourceData(name="ds1", version="1.0")],
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
        for _ in range(2):
            parsed = ParsedOTLP(
                resources=[
                    ResourceData(
                        resource_hash="same-hash", attributes={"service": "test"}
                    )
                ],
                datasources=[],
                metric_definitions=[],
                metric_data=[],
            )
            await writer.write_metrics(parsed)

        stats = writer.get_buffer_stats()
        assert stats["resources_buffered"] == 1

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


class TestIngestionConfig:
    """Tests for IngestionConfig."""

    def test_from_env_defaults(self):
        """Test default values."""
        with patch.dict(os.environ, {}, clear=True):
            config = IngestionConfig.from_env()

            assert config.write_to_datalake is True

    def test_from_env_disabled(self):
        """Test disabled writes."""
        env = {
            "WRITE_TO_DATALAKE": "false",
        }
        with patch.dict(os.environ, env, clear=True):
            config = IngestionConfig.from_env()

            assert config.write_to_datalake is False


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
            errors=[],
        )

        result = stats.to_dict()

        assert result["resources"] == 5
        assert result["datasources"] == 2
        assert result["metric_definitions"] == 10
        assert result["metric_data"] == 100
        assert result["datalake_written"] == 100
        assert result["errors"] == []

    def test_to_dict_with_errors(self):
        """Test IngestionStats.to_dict() with errors."""
        stats = IngestionStats(
            resources=0,
            datasources=0,
            metric_definitions=0,
            metric_data=0,
            datalake_written=0,
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
    def config(self):
        """Create test config."""
        return IngestionConfig(
            write_to_datalake=True,
        )

    @pytest.fixture
    def router(self, mock_datalake_writer, config):
        """Create test router."""
        return IngestionRouter(
            datalake_writer=mock_datalake_writer,
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
                                                "timeUnixNano": str(
                                                    int(
                                                        datetime.now().timestamp() * 1e9
                                                    )
                                                ),
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
                                                "timeUnixNano": str(
                                                    int(
                                                        datetime.now().timestamp() * 1e9
                                                    )
                                                ),
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
        assert "datalake" in status
        assert status["datalake"]["metric_data_buffered"] == 5

    @pytest.mark.asyncio
    async def test_router_without_datalake_writer(self):
        """Test router works without datalake writer."""
        config = IngestionConfig(write_to_datalake=True)
        router = IngestionRouter(
            datalake_writer=None,
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
                                                "timeUnixNano": str(
                                                    int(
                                                        datetime.now().timestamp() * 1e9
                                                    )
                                                ),
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
        assert "Data Lake write error" not in str(stats.errors)
