# Description: Data export and integration system for external monitoring tools
# Description: Supports Prometheus, Grafana, PowerBI, CSV/JSON, and webhook notifications

"""
Data Export and Integration System

Provides multiple export formats and integrations:
1. Prometheus - Text-based metrics format for scraping
2. Grafana SimpleJSON - Datasource API for Grafana dashboards
3. PowerBI - REST API compatible with PowerBI queries
4. CSV/JSON - General purpose data export
5. Webhooks - Alert notifications to external systems

All exporters query the normalized OTLP database schema.
"""

import json
import csv
import io
import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import psycopg2
from psycopg2.extras import RealDictCursor

# Hot cache retention limit (queries beyond this window will fail when hot cache is enabled)
HOT_CACHE_RETENTION_HOURS = int(os.getenv("HOT_CACHE_RETENTION_HOURS", "48"))
HOT_CACHE_ENABLED = os.getenv("HOT_CACHE_ENABLED", "false").lower() == "true"


class HotCacheTimeRangeError(Exception):
    """Raised when a query requests data beyond the hot cache retention window."""

    def __init__(self, requested_start: datetime, earliest_allowed: datetime):
        self.requested_start = requested_start
        self.earliest_allowed = earliest_allowed
        hours = HOT_CACHE_RETENTION_HOURS
        super().__init__(
            f"Query start time {requested_start.isoformat()} is beyond the {hours}h hot cache window. "
            f"Earliest allowed: {earliest_allowed.isoformat()}. "
            f"For historical data, use the ML training data API with Synapse."
        )


def validate_hot_cache_time_range(start_time: Optional[datetime], end_time: Optional[datetime]) -> None:
    """
    Validate that the requested time range falls within the hot cache retention window.

    When HOT_CACHE_ENABLED is true, exporters query PostgreSQL which only retains
    the last 48 hours of data. Queries requesting older data should fail with a
    clear error directing users to the Synapse/Data Lake API for historical queries.

    Args:
        start_time: Query start time (None means "now")
        end_time: Query end time (None means "now")

    Raises:
        HotCacheTimeRangeError: If start_time is older than the retention window
    """
    if not HOT_CACHE_ENABLED:
        return

    now = datetime.now(timezone.utc)
    earliest_allowed = now - timedelta(hours=HOT_CACHE_RETENTION_HOURS)

    # If no start_time specified, assume recent data (OK)
    if start_time is None:
        return

    # Ensure start_time is timezone-aware for comparison
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    if start_time < earliest_allowed:
        raise HotCacheTimeRangeError(start_time, earliest_allowed)


class ExportFormat(Enum):
    """Supported export formats."""
    PROMETHEUS = "prometheus"
    GRAFANA_JSON = "grafana_json"
    POWERBI = "powerbi"
    CSV = "csv"
    JSON = "json"


@dataclass
class MetricExport:
    """Standard metric export format."""
    metric_name: str
    resource: Dict[str, Any]
    timestamp: datetime
    value: float
    unit: Optional[str] = None
    datasource: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeSeriesQuery:
    """Query parameters for time-series data."""
    metric_names: Optional[List[str]] = None
    resource_filters: Optional[Dict[str, Any]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 1000
    aggregation: Optional[str] = None  # avg, min, max, sum
    interval: Optional[str] = None  # 1m, 5m, 1h, etc.


class DatabaseConnection:
    """Database connection helper for exporters."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(self.connection_string)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


class PrometheusExporter:
    """
    Export metrics in Prometheus text format.

    Converts database metrics to Prometheus exposition format:
    # HELP metric_name Description
    # TYPE metric_name gauge
    metric_name{label="value"} 123.45 1234567890
    """

    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string

    def export_metrics(
        self,
        query: TimeSeriesQuery,
        include_help: bool = True
    ) -> str:
        """Export metrics in Prometheus text format."""
        with DatabaseConnection(self.db_connection_string) as conn:
            metrics = self._query_metrics(conn, query)

        return self._format_prometheus(metrics, include_help)

    def _query_metrics(
        self,
        conn,
        query: TimeSeriesQuery
    ) -> List[MetricExport]:
        """Query metrics from database."""
        # Validate time range is within hot cache window
        validate_hot_cache_time_range(query.start_time, query.end_time)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Build SQL query
            sql = """
                SELECT
                    md.name as metric_name,
                    md.unit,
                    md.description,
                    ds.name as datasource_name,
                    r.attributes as resource_attrs,
                    m.timestamp,
                    COALESCE(m.value_double, m.value_int::float) as value,
                    m.attributes as metric_attrs
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN datasources ds ON md.datasource_id = ds.id
                JOIN resources r ON m.resource_id = r.id
                WHERE 1=1
            """
            params = []

            # Add filters
            if query.metric_names:
                placeholders = ','.join(['%s'] * len(query.metric_names))
                sql += f" AND md.name IN ({placeholders})"
                params.extend(query.metric_names)

            if query.start_time:
                sql += " AND m.timestamp >= %s"
                params.append(query.start_time)

            if query.end_time:
                sql += " AND m.timestamp <= %s"
                params.append(query.end_time)

            # Order and limit
            sql += " ORDER BY m.timestamp DESC LIMIT %s"
            params.append(query.limit)

            cur.execute(sql, params)
            rows = cur.fetchall()

            return [
                MetricExport(
                    metric_name=row['metric_name'],
                    resource=row['resource_attrs'],
                    timestamp=row['timestamp'],
                    value=row['value'],
                    unit=row['unit'],
                    datasource=row['datasource_name'],
                    attributes=row['metric_attrs'] or {}
                )
                for row in rows
            ]

    def _format_prometheus(
        self,
        metrics: List[MetricExport],
        include_help: bool
    ) -> str:
        """Format metrics as Prometheus text."""
        lines = []
        metrics_by_name = {}

        # Group metrics by name
        for m in metrics:
            if m.metric_name not in metrics_by_name:
                metrics_by_name[m.metric_name] = []
            metrics_by_name[m.metric_name].append(m)

        # Format each metric group
        for metric_name, metric_list in metrics_by_name.items():
            # Sanitize metric name for Prometheus
            prom_name = self._sanitize_metric_name(metric_name)

            if include_help and metric_list:
                first = metric_list[0]
                unit_str = f" ({first.unit})" if first.unit else ""
                lines.append(f"# HELP {prom_name} {first.datasource or metric_name}{unit_str}")
                lines.append(f"# TYPE {prom_name} gauge")

            # Export each data point
            for m in metric_list:
                labels = self._build_labels(m)
                timestamp_ms = int(m.timestamp.timestamp() * 1000)
                lines.append(f"{prom_name}{{{labels}}} {m.value} {timestamp_ms}")

        return '\n'.join(lines) + '\n'

    def _sanitize_metric_name(self, name: str) -> str:
        """Sanitize metric name for Prometheus."""
        # Replace invalid characters with underscores
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
        # Ensure starts with letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        return sanitized or 'unknown'

    def _build_labels(self, metric: MetricExport) -> str:
        """Build Prometheus labels from resource and attributes."""
        labels = {}

        # Add resource attributes as labels
        for key, value in metric.resource.items():
            label_name = self._sanitize_metric_name(key)
            labels[label_name] = str(value)

        # Add datasource
        if metric.datasource:
            labels['datasource'] = metric.datasource

        # Add metric attributes
        for key, value in metric.attributes.items():
            label_name = self._sanitize_metric_name(key)
            labels[label_name] = str(value)

        # Format as Prometheus labels
        label_pairs = [f'{k}="{v}"' for k, v in sorted(labels.items())]
        return ','.join(label_pairs)


class GrafanaSimpleJSONDataSource:
    """
    Grafana SimpleJSON datasource implementation.

    Implements the SimpleJSON datasource API:
    - / : Health check
    - /search : Return available metrics
    - /query : Return time-series data
    - /annotations : Return annotations (alerts)
    """

    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string

    def health_check(self) -> Dict[str, Any]:
        """Health check endpoint."""
        try:
            with DatabaseConnection(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def search(self, target: Optional[str] = None) -> List[str]:
        """Return list of available metrics."""
        with DatabaseConnection(self.db_connection_string) as conn:
            with conn.cursor() as cur:
                if target:
                    cur.execute(
                        "SELECT DISTINCT name FROM metric_definitions WHERE name LIKE %s ORDER BY name",
                        (f"%{target}%",)
                    )
                else:
                    cur.execute("SELECT DISTINCT name FROM metric_definitions ORDER BY name")

                return [row[0] for row in cur.fetchall()]

    def query(self, query_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Handle Grafana query request.

        Request format:
        {
            "targets": [{"target": "metric_name", "refId": "A"}],
            "range": {"from": "2023-01-01T00:00:00Z", "to": "2023-01-02T00:00:00Z"},
            "interval": "1m"
        }
        """
        targets = query_request.get('targets', [])
        time_range = query_request.get('range', {})
        interval = query_request.get('interval', '1m')

        # Parse time range
        start_time = self._parse_datetime(time_range.get('from'))
        end_time = self._parse_datetime(time_range.get('to'))

        results = []

        with DatabaseConnection(self.db_connection_string) as conn:
            for target_obj in targets:
                target = target_obj.get('target')
                if not target:
                    continue

                # Query metric data
                datapoints = self._query_timeseries(
                    conn, target, start_time, end_time, interval
                )

                results.append({
                    "target": target,
                    "datapoints": datapoints
                })

        return results

    def _query_timeseries(
        self,
        conn,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        interval: str
    ) -> List[List]:
        """Query time-series data."""
        # Validate time range is within hot cache window
        validate_hot_cache_time_range(start_time, end_time)

        with conn.cursor() as cur:
            sql = """
                SELECT
                    m.timestamp,
                    AVG(COALESCE(m.value_double, m.value_int::float)) as value
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                WHERE md.name = %s
                    AND m.timestamp >= %s
                    AND m.timestamp <= %s
                GROUP BY m.timestamp
                ORDER BY m.timestamp
            """

            cur.execute(sql, (metric_name, start_time, end_time))
            rows = cur.fetchall()

            # Format as Grafana datapoints: [[value, timestamp_ms], ...]
            return [
                [float(row[1]), int(row[0].timestamp() * 1000)]
                for row in rows
            ]

    def annotations(self, annotation_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Return annotations (alerts/events).

        Request format:
        {
            "range": {"from": "...", "to": "..."},
            "annotation": {"name": "...", "query": "..."}
        }
        """
        # FUTURE: Integration point for anomaly detection system
        # When ML features are enabled, this endpoint will return annotations for:
        # - Detected anomalies from AnomalyDetector
        # - Significant metric changes
        # - System events and alerts
        # For now, returning empty array (Grafana handles this gracefully)
        return []

    def _parse_datetime(self, dt_str: Optional[str]) -> datetime:
        """Parse datetime string from Grafana."""
        if not dt_str:
            return datetime.now()

        # Handle ISO format
        if 'T' in dt_str:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

        # Handle timestamp
        try:
            return datetime.fromtimestamp(int(dt_str) / 1000)
        except (ValueError, TypeError, OSError):
            return datetime.now()


class PowerBIExporter:
    """
    PowerBI REST API compatible exporter.

    Returns data in a format compatible with PowerBI's REST API connector:
    {
        "value": [
            {"metric": "cpu.usage", "timestamp": "2023-01-01T00:00:00Z", "value": 50.0, ...},
            ...
        ],
        "@odata.nextLink": "..."
    }
    """

    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string

    def export_data(
        self,
        query: TimeSeriesQuery,
        skip: int = 0,
        top: int = 1000
    ) -> Dict[str, Any]:
        """
        Export data in PowerBI-compatible format.

        Supports OData-style pagination with $skip and $top.
        """
        with DatabaseConnection(self.db_connection_string) as conn:
            metrics, total_count = self._query_metrics_paginated(
                conn, query, skip, top
            )

        # Format as PowerBI expects
        result = {
            "value": [self._format_metric_powerbi(m) for m in metrics],
            "@odata.count": total_count
        }

        # Add next link if more data available
        if skip + top < total_count:
            result["@odata.nextLink"] = f"?$skip={skip + top}&$top={top}"

        return result

    def _query_metrics_paginated(
        self,
        conn,
        query: TimeSeriesQuery,
        skip: int,
        top: int
    ) -> Tuple[List[MetricExport], int]:
        """Query metrics with pagination."""
        # Validate time range is within hot cache window
        validate_hot_cache_time_range(query.start_time, query.end_time)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Count total
            count_sql = """
                SELECT COUNT(*) as total
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                WHERE 1=1
            """
            count_params = []

            if query.metric_names:
                placeholders = ','.join(['%s'] * len(query.metric_names))
                count_sql += f" AND md.name IN ({placeholders})"
                count_params.extend(query.metric_names)

            if query.start_time:
                count_sql += " AND m.timestamp >= %s"
                count_params.append(query.start_time)

            if query.end_time:
                count_sql += " AND m.timestamp <= %s"
                count_params.append(query.end_time)

            cur.execute(count_sql, count_params)
            total_count = cur.fetchone()['total']

            # Query data
            sql = """
                SELECT
                    md.name as metric_name,
                    md.unit,
                    ds.name as datasource_name,
                    r.attributes as resource_attrs,
                    m.timestamp,
                    COALESCE(m.value_double, m.value_int::float) as value,
                    m.attributes as metric_attrs
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN datasources ds ON md.datasource_id = ds.id
                JOIN resources r ON m.resource_id = r.id
                WHERE 1=1
            """
            params = []

            # Add same filters as count query
            if query.metric_names:
                placeholders = ','.join(['%s'] * len(query.metric_names))
                sql += f" AND md.name IN ({placeholders})"
                params.extend(query.metric_names)

            if query.start_time:
                sql += " AND m.timestamp >= %s"
                params.append(query.start_time)

            if query.end_time:
                sql += " AND m.timestamp <= %s"
                params.append(query.end_time)

            sql += " ORDER BY m.timestamp DESC OFFSET %s LIMIT %s"
            params.extend([skip, top])

            cur.execute(sql, params)
            rows = cur.fetchall()

            metrics = [
                MetricExport(
                    metric_name=row['metric_name'],
                    resource=row['resource_attrs'],
                    timestamp=row['timestamp'],
                    value=row['value'],
                    unit=row['unit'],
                    datasource=row['datasource_name'],
                    attributes=row['metric_attrs'] or {}
                )
                for row in rows
            ]

            return metrics, total_count

    def _format_metric_powerbi(self, metric: MetricExport) -> Dict[str, Any]:
        """Format metric for PowerBI."""
        result = {
            "metric": metric.metric_name,
            "timestamp": metric.timestamp.isoformat(),
            "value": metric.value
        }

        if metric.unit:
            result["unit"] = metric.unit

        if metric.datasource:
            result["datasource"] = metric.datasource

        # Flatten resource attributes
        for key, value in metric.resource.items():
            result[f"resource_{key}"] = value

        # Flatten metric attributes
        for key, value in metric.attributes.items():
            result[f"attr_{key}"] = value

        return result


class CSVJSONExporter:
    """
    General purpose CSV and JSON exporters.

    Provides flexible data export in standard formats.
    """

    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string

    def export_csv(
        self,
        query: TimeSeriesQuery,
        flatten_json: bool = True
    ) -> str:
        """Export metrics as CSV."""
        with DatabaseConnection(self.db_connection_string) as conn:
            metrics = self._query_metrics(conn, query)

        if not metrics:
            return ""

        # Flatten metrics to dictionaries
        rows = [self._flatten_metric(m, flatten_json) for m in metrics]

        # Write CSV
        output = io.StringIO()
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

        return output.getvalue()

    def export_json(
        self,
        query: TimeSeriesQuery,
        pretty: bool = False
    ) -> str:
        """Export metrics as JSON."""
        with DatabaseConnection(self.db_connection_string) as conn:
            metrics = self._query_metrics(conn, query)

        # Convert to JSON-serializable format
        data = [self._metric_to_dict(m) for m in metrics]

        indent = 2 if pretty else None
        return json.dumps(data, indent=indent, default=str)

    def _query_metrics(
        self,
        conn,
        query: TimeSeriesQuery
    ) -> List[MetricExport]:
        """Query metrics from database."""
        # Validate time range is within hot cache window
        validate_hot_cache_time_range(query.start_time, query.end_time)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT
                    md.name as metric_name,
                    md.unit,
                    ds.name as datasource_name,
                    r.attributes as resource_attrs,
                    m.timestamp,
                    COALESCE(m.value_double, m.value_int::float) as value,
                    m.attributes as metric_attrs
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN datasources ds ON md.datasource_id = ds.id
                JOIN resources r ON m.resource_id = r.id
                WHERE 1=1
            """
            params = []

            if query.metric_names:
                placeholders = ','.join(['%s'] * len(query.metric_names))
                sql += f" AND md.name IN ({placeholders})"
                params.extend(query.metric_names)

            if query.start_time:
                sql += " AND m.timestamp >= %s"
                params.append(query.start_time)

            if query.end_time:
                sql += " AND m.timestamp <= %s"
                params.append(query.end_time)

            sql += " ORDER BY m.timestamp DESC LIMIT %s"
            params.append(query.limit)

            cur.execute(sql, params)
            rows = cur.fetchall()

            return [
                MetricExport(
                    metric_name=row['metric_name'],
                    resource=row['resource_attrs'],
                    timestamp=row['timestamp'],
                    value=row['value'],
                    unit=row['unit'],
                    datasource=row['datasource_name'],
                    attributes=row['metric_attrs'] or {}
                )
                for row in rows
            ]

    def _flatten_metric(
        self,
        metric: MetricExport,
        flatten_json: bool
    ) -> Dict[str, Any]:
        """Flatten metric to a single-level dictionary."""
        result = {
            "metric_name": metric.metric_name,
            "timestamp": metric.timestamp.isoformat(),
            "value": metric.value,
            "unit": metric.unit or "",
            "datasource": metric.datasource or ""
        }

        if flatten_json:
            # Flatten resource attributes
            for key, value in metric.resource.items():
                result[f"resource_{key}"] = value

            # Flatten metric attributes
            for key, value in metric.attributes.items():
                result[f"attr_{key}"] = value
        else:
            result["resource"] = json.dumps(metric.resource)
            result["attributes"] = json.dumps(metric.attributes)

        return result

    def _metric_to_dict(self, metric: MetricExport) -> Dict[str, Any]:
        """Convert metric to dictionary."""
        return {
            "metric_name": metric.metric_name,
            "timestamp": metric.timestamp.isoformat(),
            "value": metric.value,
            "unit": metric.unit,
            "datasource": metric.datasource,
            "resource": metric.resource,
            "attributes": metric.attributes
        }


