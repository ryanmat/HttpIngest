"""
Test suite for data exporters and integrations.

Tests all export formats: Prometheus, Grafana, PowerBI, CSV/JSON, and Webhooks.
"""

import pytest
import json
import csv
import io
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from src.exporters import (
    PrometheusExporter,
    GrafanaSimpleJSONDataSource,
    PowerBIExporter,
    CSVJSONExporter,
    WebhookNotifier,
    TimeSeriesQuery,
    AlertEvent,
    WebhookConfig,
    MetricExport
)


@pytest.fixture
def db_connection_string():
    """Database connection string for tests."""
    return "postgresql://test:test@localhost:5432/testdb"


@pytest.fixture
def sample_query():
    """Sample time-series query."""
    return TimeSeriesQuery(
        metric_names=["cpu.usage", "memory.usage"],
        start_time=datetime.now() - timedelta(hours=1),
        end_time=datetime.now(),
        limit=100
    )


@pytest.fixture
def sample_metrics():
    """Sample metrics for testing."""
    now = datetime.now()
    return [
        MetricExport(
            metric_name="cpu.usage",
            resource={"service.name": "web-server", "host.name": "server-01"},
            timestamp=now - timedelta(minutes=5),
            value=75.5,
            unit="percent",
            datasource="CPU_Usage"
        ),
        MetricExport(
            metric_name="cpu.usage",
            resource={"service.name": "web-server", "host.name": "server-01"},
            timestamp=now,
            value=80.2,
            unit="percent",
            datasource="CPU_Usage"
        ),
        MetricExport(
            metric_name="memory.usage",
            resource={"service.name": "web-server", "host.name": "server-01"},
            timestamp=now,
            value=4096.0,
            unit="MB",
            datasource="Memory_Usage"
        )
    ]


class TestPrometheusExporter:
    """Test Prometheus metrics exporter."""

    def test_sanitize_metric_name(self):
        """Test metric name sanitization."""
        exporter = PrometheusExporter("dummy")

        assert exporter._sanitize_metric_name("cpu.usage") == "cpu_usage"
        assert exporter._sanitize_metric_name("http-requests") == "http_requests"
        assert exporter._sanitize_metric_name("123metric") == "_123metric"
        assert exporter._sanitize_metric_name("valid_metric") == "valid_metric"

    def test_build_labels(self, sample_metrics):
        """Test Prometheus label building."""
        exporter = PrometheusExporter("dummy")
        metric = sample_metrics[0]

        labels = exporter._build_labels(metric)

        assert 'service_name="web-server"' in labels
        assert 'host_name="server-01"' in labels
        assert 'datasource="CPU_Usage"' in labels

    def test_format_prometheus(self, sample_metrics):
        """Test Prometheus text format."""
        exporter = PrometheusExporter("dummy")

        output = exporter._format_prometheus(sample_metrics, include_help=True)

        # Check HELP and TYPE lines
        assert "# HELP cpu_usage" in output
        assert "# TYPE cpu_usage gauge" in output
        assert "# HELP memory_usage" in output
        assert "# TYPE memory_usage gauge" in output

        # Check metric lines
        assert "cpu_usage{" in output
        assert "memory_usage{" in output
        assert "75.5" in output
        assert "80.2" in output
        assert "4096.0" in output

    def test_format_prometheus_without_help(self, sample_metrics):
        """Test Prometheus format without HELP."""
        exporter = PrometheusExporter("dummy")

        output = exporter._format_prometheus(sample_metrics, include_help=False)

        assert "# HELP" not in output
        assert "# TYPE" not in output
        assert "cpu_usage{" in output


class TestGrafanaSimpleJSON:
    """Test Grafana SimpleJSON datasource."""

    def test_health_check_success(self, db_connection_string):
        """Test health check endpoint."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        # Mock successful database connection
        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            result = datasource.health_check()

            assert result["status"] == "ok"

    def test_health_check_failure(self, db_connection_string):
        """Test health check with database error."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_db.return_value.__enter__.side_effect = Exception("Connection failed")

            result = datasource.health_check()

            assert result["status"] == "error"
            assert "Connection failed" in result["message"]

    def test_search_all_metrics(self, db_connection_string):
        """Test search endpoint without filter."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                ("cpu.usage",),
                ("memory.usage",),
                ("disk.io",)
            ]
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_db.return_value.__enter__.return_value = mock_conn

            result = datasource.search()

            assert result == ["cpu.usage", "memory.usage", "disk.io"]

    def test_search_with_filter(self, db_connection_string):
        """Test search endpoint with filter."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [("cpu.usage",), ("cpu.idle",)]
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_db.return_value.__enter__.return_value = mock_conn

            result = datasource.search(target="cpu")

            assert result == ["cpu.usage", "cpu.idle"]
            # Verify LIKE query was used
            mock_cursor.execute.assert_called_once()
            assert "%cpu%" in str(mock_cursor.execute.call_args)

    def test_query_request(self, db_connection_string):
        """Test Grafana query endpoint."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        query_request = {
            "targets": [
                {"target": "cpu.usage", "refId": "A"}
            ],
            "range": {
                "from": "2023-01-01T00:00:00Z",
                "to": "2023-01-01T01:00:00Z"
            },
            "interval": "1m"
        }

        now = datetime(2023, 1, 1, 0, 30)
        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                (now, 75.5),
                (now + timedelta(minutes=1), 76.0)
            ]
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_db.return_value.__enter__.return_value = mock_conn

            result = datasource.query(query_request)

            assert len(result) == 1
            assert result[0]["target"] == "cpu.usage"
            assert len(result[0]["datapoints"]) == 2
            # Verify datapoint format [value, timestamp_ms]
            assert result[0]["datapoints"][0][0] == 75.5
            assert isinstance(result[0]["datapoints"][0][1], int)

    def test_parse_datetime_iso(self, db_connection_string):
        """Test datetime parsing from ISO format."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        dt = datasource._parse_datetime("2023-01-01T00:00:00Z")

        assert dt.year == 2023
        assert dt.month == 1
        assert dt.day == 1

    def test_parse_datetime_timestamp(self, db_connection_string):
        """Test datetime parsing from timestamp."""
        datasource = GrafanaSimpleJSONDataSource(db_connection_string)

        # 1672531200000 = 2023-01-01T00:00:00 UTC in milliseconds
        dt = datasource._parse_datetime("1672531200000")

        # Just verify it's a valid datetime, timezone may vary
        assert isinstance(dt, datetime)
        assert dt.year in [2022, 2023]  # Allow for timezone differences


class TestPowerBIExporter:
    """Test PowerBI REST API exporter."""

    def test_export_data_basic(self, db_connection_string, sample_metrics):
        """Test basic data export."""
        exporter = PowerBIExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics_paginated') as mock_query:
                mock_query.return_value = (sample_metrics[:2], 2)

                result = exporter.export_data(
                    TimeSeriesQuery(metric_names=["cpu.usage"]),
                    skip=0,
                    top=10
                )

                assert "value" in result
                assert "@odata.count" in result
                assert len(result["value"]) == 2
                assert result["@odata.count"] == 2

    def test_export_data_with_pagination(self, db_connection_string, sample_metrics):
        """Test data export with pagination."""
        exporter = PowerBIExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics_paginated') as mock_query:
                mock_query.return_value = (sample_metrics[:2], 100)

                result = exporter.export_data(
                    TimeSeriesQuery(),
                    skip=0,
                    top=2
                )

                assert "@odata.nextLink" in result
                assert "$skip=2" in result["@odata.nextLink"]
                assert "$top=2" in result["@odata.nextLink"]

    def test_export_data_last_page(self, db_connection_string, sample_metrics):
        """Test last page has no next link."""
        exporter = PowerBIExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics_paginated') as mock_query:
                mock_query.return_value = (sample_metrics[:2], 2)

                result = exporter.export_data(
                    TimeSeriesQuery(),
                    skip=0,
                    top=10
                )

                assert "@odata.nextLink" not in result

    def test_format_metric_powerbi(self, db_connection_string, sample_metrics):
        """Test PowerBI metric formatting."""
        exporter = PowerBIExporter(db_connection_string)
        metric = sample_metrics[0]

        formatted = exporter._format_metric_powerbi(metric)

        assert formatted["metric"] == "cpu.usage"
        assert formatted["value"] == 75.5
        assert formatted["unit"] == "percent"
        assert formatted["datasource"] == "CPU_Usage"
        assert formatted["resource_service.name"] == "web-server"
        assert formatted["resource_host.name"] == "server-01"
        assert "timestamp" in formatted


class TestCSVJSONExporter:
    """Test CSV and JSON exporters."""

    def test_export_csv_basic(self, db_connection_string, sample_metrics):
        """Test basic CSV export."""
        exporter = CSVJSONExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics') as mock_query:
                mock_query.return_value = sample_metrics

                csv_output = exporter.export_csv(
                    TimeSeriesQuery(metric_names=["cpu.usage"])
                )

                # Parse CSV
                reader = csv.DictReader(io.StringIO(csv_output))
                rows = list(reader)

                assert len(rows) == 3
                assert "metric_name" in rows[0]
                assert "value" in rows[0]
                assert rows[0]["metric_name"] == "cpu.usage"
                assert float(rows[0]["value"]) == 75.5

    def test_export_csv_flattened(self, db_connection_string, sample_metrics):
        """Test CSV export with flattened JSON."""
        exporter = CSVJSONExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics') as mock_query:
                mock_query.return_value = [sample_metrics[0]]

                csv_output = exporter.export_csv(
                    TimeSeriesQuery(),
                    flatten_json=True
                )

                reader = csv.DictReader(io.StringIO(csv_output))
                rows = list(reader)

                # Check flattened resource attributes
                assert "resource_service.name" in rows[0]
                assert "resource_host.name" in rows[0]
                assert rows[0]["resource_service.name"] == "web-server"

    def test_export_csv_empty(self, db_connection_string):
        """Test CSV export with no data."""
        exporter = CSVJSONExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics') as mock_query:
                mock_query.return_value = []

                csv_output = exporter.export_csv(TimeSeriesQuery())

                assert csv_output == ""

    def test_export_json_basic(self, db_connection_string, sample_metrics):
        """Test basic JSON export."""
        exporter = CSVJSONExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics') as mock_query:
                mock_query.return_value = sample_metrics

                json_output = exporter.export_json(TimeSeriesQuery())

                data = json.loads(json_output)

                assert len(data) == 3
                assert data[0]["metric_name"] == "cpu.usage"
                assert data[0]["value"] == 75.5
                assert data[0]["resource"]["service.name"] == "web-server"

    def test_export_json_pretty(self, db_connection_string, sample_metrics):
        """Test pretty JSON export."""
        exporter = CSVJSONExporter(db_connection_string)

        with patch('src.exporters.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            with patch.object(exporter, '_query_metrics') as mock_query:
                mock_query.return_value = [sample_metrics[0]]

                json_output = exporter.export_json(TimeSeriesQuery(), pretty=True)

                # Pretty JSON should have newlines and indentation
                assert "\n" in json_output
                assert "  " in json_output

    def test_metric_to_dict(self, db_connection_string, sample_metrics):
        """Test metric to dictionary conversion."""
        exporter = CSVJSONExporter(db_connection_string)
        metric = sample_metrics[0]

        result = exporter._metric_to_dict(metric)

        assert result["metric_name"] == "cpu.usage"
        assert result["value"] == 75.5
        assert result["unit"] == "percent"
        assert isinstance(result["resource"], dict)
        assert isinstance(result["attributes"], dict)


class TestWebhookNotifier:
    """Test webhook notification system."""

    def test_send_alert_success(self):
        """Test successful alert delivery."""
        notifier = WebhookNotifier()

        alert = AlertEvent(
            alert_id="alert-123",
            severity="critical",
            metric_name="cpu.usage",
            resource={"host": "server-01"},
            current_value=95.0,
            threshold=90.0,
            message="CPU usage critically high",
            timestamp=datetime.now()
        )

        webhook_config = WebhookConfig(
            url="https://example.com/webhook",
            method="POST"
        )

        with patch.object(notifier, '_send_request') as mock_send:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = "OK"
            mock_send.return_value = mock_response

            result = notifier.send_alert(alert, webhook_config)

            assert result["success"] is True
            assert result["status_code"] == 200
            assert result["attempt"] == 1

    def test_send_alert_with_hmac(self):
        """Test alert with HMAC signature."""
        notifier = WebhookNotifier()

        alert = AlertEvent(
            alert_id="alert-123",
            severity="warning",
            metric_name="memory.usage",
            resource={"host": "server-01"},
            current_value=85.0,
            threshold=80.0,
            message="Memory usage high",
            timestamp=datetime.now()
        )

        webhook_config = WebhookConfig(
            url="https://example.com/webhook",
            secret="my-secret-key"
        )

        with patch.object(notifier, '_send_request') as mock_send:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = "OK"
            mock_send.return_value = mock_response

            result = notifier.send_alert(alert, webhook_config)

            assert result["success"] is True
            # Verify HMAC signature was computed
            call_args = mock_send.call_args
            headers = call_args[0][3]
            assert 'X-Webhook-Signature' in headers
            assert headers['X-Webhook-Signature'].startswith('sha256=')

    def test_send_alert_retry(self):
        """Test alert retry on failure."""
        notifier = WebhookNotifier()

        alert = AlertEvent(
            alert_id="alert-123",
            severity="info",
            metric_name="disk.usage",
            resource={"host": "server-01"},
            current_value=70.0,
            threshold=75.0,
            message="Disk usage normal",
            timestamp=datetime.now()
        )

        webhook_config = WebhookConfig(
            url="https://example.com/webhook",
            retry_count=3
        )

        with patch.object(notifier, '_send_request') as mock_send:
            # Fail twice, succeed on third attempt
            mock_send.side_effect = [
                Exception("Connection timeout"),
                Exception("Connection timeout"),
                Mock(status_code=200, text="OK")
            ]

            result = notifier.send_alert(alert, webhook_config)

            assert result["success"] is True
            assert result["attempt"] == 3
            assert mock_send.call_count == 3

    def test_send_alert_max_retries(self):
        """Test alert failure after max retries."""
        notifier = WebhookNotifier()

        alert = AlertEvent(
            alert_id="alert-123",
            severity="critical",
            metric_name="cpu.usage",
            resource={"host": "server-01"},
            current_value=99.0,
            threshold=90.0,
            message="CPU at max",
            timestamp=datetime.now()
        )

        webhook_config = WebhookConfig(
            url="https://example.com/webhook",
            retry_count=2
        )

        with patch.object(notifier, '_send_request') as mock_send:
            mock_send.side_effect = Exception("Connection failed")

            result = notifier.send_alert(alert, webhook_config)

            assert result["success"] is False
            assert "error" in result
            assert mock_send.call_count == 2

    def test_prepare_payload(self):
        """Test webhook payload preparation."""
        notifier = WebhookNotifier()

        alert = AlertEvent(
            alert_id="alert-123",
            severity="warning",
            metric_name="cpu.usage",
            resource={"host": "server-01"},
            current_value=85.0,
            threshold=80.0,
            message="CPU high",
            timestamp=datetime(2023, 1, 1, 12, 0, 0),
            anomaly_score=0.95
        )

        payload = notifier._prepare_payload(alert)
        data = json.loads(payload)

        assert data["alert_id"] == "alert-123"
        assert data["severity"] == "warning"
        assert data["metric_name"] == "cpu.usage"
        assert data["current_value"] == 85.0
        assert data["threshold"] == 80.0
        assert data["anomaly_score"] == 0.95

    def test_compute_hmac(self):
        """Test HMAC signature computation."""
        notifier = WebhookNotifier()

        payload = '{"test": "data"}'
        secret = "my-secret"

        signature = notifier._compute_hmac(payload, secret)

        assert signature.startswith("sha256=")
        # Verify it's a hex string
        hex_part = signature.split("=")[1]
        assert len(hex_part) == 64  # SHA256 is 256 bits = 64 hex chars

    def test_send_request_post(self):
        """Test POST request."""
        notifier = WebhookNotifier()

        with patch.object(notifier.session, 'post') as mock_post:
            mock_response = Mock()
            mock_post.return_value = mock_response

            result = notifier._send_request(
                "https://example.com/webhook",
                "POST",
                '{"test": "data"}',
                {"X-Custom": "Header"},
                10
            )

            assert result == mock_response
            mock_post.assert_called_once()

    def test_send_request_put(self):
        """Test PUT request."""
        notifier = WebhookNotifier()

        with patch.object(notifier.session, 'put') as mock_put:
            mock_response = Mock()
            mock_put.return_value = mock_response

            result = notifier._send_request(
                "https://example.com/webhook",
                "PUT",
                '{"test": "data"}',
                {},
                10
            )

            assert result == mock_response
            mock_put.assert_called_once()

    def test_send_request_invalid_method(self):
        """Test unsupported HTTP method."""
        notifier = WebhookNotifier()

        with pytest.raises(ValueError, match="Unsupported HTTP method"):
            notifier._send_request(
                "https://example.com/webhook",
                "DELETE",
                '{"test": "data"}',
                {},
                10
            )


class TestTimeSeriesQuery:
    """Test TimeSeriesQuery dataclass."""

    def test_default_values(self):
        """Test query with default values."""
        query = TimeSeriesQuery()

        assert query.metric_names is None
        assert query.resource_filters is None
        assert query.start_time is None
        assert query.end_time is None
        assert query.limit == 1000
        assert query.aggregation is None
        assert query.interval is None

    def test_custom_values(self):
        """Test query with custom values."""
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)

        query = TimeSeriesQuery(
            metric_names=["cpu.usage"],
            start_time=start,
            end_time=end,
            limit=500,
            aggregation="avg",
            interval="5m"
        )

        assert query.metric_names == ["cpu.usage"]
        assert query.start_time == start
        assert query.end_time == end
        assert query.limit == 500
        assert query.aggregation == "avg"
        assert query.interval == "5m"


class TestAlertEvent:
    """Test AlertEvent dataclass."""

    def test_alert_event_creation(self):
        """Test alert event creation."""
        now = datetime.now()

        alert = AlertEvent(
            alert_id="alert-123",
            severity="critical",
            metric_name="cpu.usage",
            resource={"host": "server-01"},
            current_value=95.0,
            threshold=90.0,
            message="CPU critically high",
            timestamp=now,
            anomaly_score=0.98,
            metadata={"source": "LSTM"}
        )

        assert alert.alert_id == "alert-123"
        assert alert.severity == "critical"
        assert alert.anomaly_score == 0.98
        assert alert.metadata["source"] == "LSTM"
