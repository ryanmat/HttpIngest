# Description: Parser for LogicMonitor OTLP (OpenTelemetry Protocol) JSON data
# Description: Extracts resources, datasources, metrics, and time-series data for database insertion

"""
OTLP Parser for LogicMonitor Data Pipeline

Parses OTLP JSON payloads into structured data ready for database insertion.

The parser handles:
- Resource attributes (device/service information)
- Datasource metadata (scopes)
- Metric definitions (names, types, units)
- Time-series data points (gauge, sum, histogram, etc.)

All functions are pure (no side effects) and return structured data.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict


@dataclass
class ResourceData:
    """Represents a parsed OTLP resource."""
    resource_hash: str
    attributes: Dict[str, Any]


@dataclass
class DatasourceData:
    """Represents a parsed OTLP datasource (scope)."""
    name: str
    version: Optional[str]


@dataclass
class MetricDefinitionData:
    """Represents a parsed metric definition."""
    datasource_name: str
    datasource_version: Optional[str]
    name: str
    unit: Optional[str]
    metric_type: str
    description: Optional[str]


@dataclass
class MetricDataPoint:
    """Represents a parsed time-series data point."""
    resource_hash: str
    datasource_name: str
    datasource_version: Optional[str]
    metric_name: str
    timestamp: datetime
    value_double: Optional[float]
    value_int: Optional[int]
    attributes: Optional[Dict[str, Any]]


@dataclass
class ParsedOTLP:
    """Complete parsed OTLP data structure."""
    resources: List[ResourceData]
    datasources: List[DatasourceData]
    metric_definitions: List[MetricDefinitionData]
    metric_data: List[MetricDataPoint]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for easier testing/serialization."""
        return {
            'resources': [asdict(r) for r in self.resources],
            'datasources': [asdict(d) for d in self.datasources],
            'metric_definitions': [asdict(m) for m in self.metric_definitions],
            'metric_data': [asdict(m) for m in self.metric_data],
        }


def extract_attribute_value(attr_value: Dict[str, Any]) -> Any:
    """
    Extract value from OTLP attribute value object.

    OTLP attributes have format: {"stringValue": "foo"} or {"intValue": 123}

    Args:
        attr_value: OTLP attribute value dict

    Returns:
        Extracted value (string, int, bool, etc.)
    """
    # OTLP attribute value types
    if 'stringValue' in attr_value:
        return attr_value['stringValue']
    elif 'intValue' in attr_value:
        return attr_value['intValue']
    elif 'doubleValue' in attr_value:
        return attr_value['doubleValue']
    elif 'boolValue' in attr_value:
        return attr_value['boolValue']
    elif 'bytesValue' in attr_value:
        return attr_value['bytesValue']
    elif 'arrayValue' in attr_value:
        # Array of values
        return [extract_attribute_value(v) for v in attr_value['arrayValue'].get('values', [])]
    elif 'kvlistValue' in attr_value:
        # Key-value list
        return {
            kv['key']: extract_attribute_value(kv['value'])
            for kv in attr_value['kvlistValue'].get('values', [])
        }
    else:
        # Unknown type, return as-is
        return attr_value


def parse_resource_attributes(resource: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse OTLP resource attributes into a flat dictionary.

    Args:
        resource: OTLP resource object with 'attributes' array

    Returns:
        Dictionary of attribute key-value pairs
    """
    attributes = {}

    for attr in resource.get('attributes', []):
        key = attr.get('key')
        value = attr.get('value', {})

        if key:
            attributes[key] = extract_attribute_value(value)

    return attributes


def compute_resource_hash(attributes: Dict[str, Any]) -> str:
    """
    Compute SHA256 hash of resource attributes for deduplication.

    Args:
        attributes: Resource attributes dictionary

    Returns:
        SHA256 hash as hex string
    """
    # Sort keys for consistent hashing
    attr_str = json.dumps(attributes, sort_keys=True)
    return hashlib.sha256(attr_str.encode()).hexdigest()


def convert_nano_timestamp(time_unix_nano) -> datetime:
    """
    Convert OTLP nanosecond timestamp to Python datetime.

    Args:
        time_unix_nano: Unix timestamp in nanoseconds (int or str)

    Returns:
        Timezone-aware datetime object (UTC)
    """
    # LogicMonitor sends timestamps as strings, so convert if needed
    if isinstance(time_unix_nano, str):
        time_unix_nano = int(time_unix_nano)

    # Convert nanoseconds to seconds (divide by 1e9)
    timestamp_seconds = time_unix_nano / 1e9
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)


def parse_data_point(
    data_point: Dict[str, Any],
    resource_hash: str,
    datasource_name: str,
    datasource_version: Optional[str],
    metric_name: str
) -> MetricDataPoint:
    """
    Parse a single OTLP data point.

    Args:
        data_point: OTLP data point object
        resource_hash: Hash of the resource this point belongs to
        datasource_name: Name of the datasource
        datasource_version: Version of the datasource
        metric_name: Name of the metric

    Returns:
        MetricDataPoint object
    """
    # Extract timestamp
    time_unix_nano = data_point.get('timeUnixNano', 0)
    timestamp = convert_nano_timestamp(time_unix_nano)

    # Extract value (either asDouble or asInt)
    # LogicMonitor may send these as strings or numbers
    value_double = data_point.get('asDouble')
    if value_double is not None and isinstance(value_double, str):
        value_double = float(value_double)

    value_int = data_point.get('asInt')
    if value_int is not None and isinstance(value_int, str):
        value_int = int(value_int)

    # Extract additional attributes if present
    attributes = None
    if 'attributes' in data_point:
        attributes = {}
        for attr in data_point['attributes']:
            key = attr.get('key')
            value = attr.get('value', {})
            if key:
                attributes[key] = extract_attribute_value(value)

    return MetricDataPoint(
        resource_hash=resource_hash,
        datasource_name=datasource_name,
        datasource_version=datasource_version,
        metric_name=metric_name,
        timestamp=timestamp,
        value_double=value_double,
        value_int=value_int,
        attributes=attributes
    )


def parse_metric(
    metric: Dict[str, Any],
    resource_hash: str,
    datasource_name: str,
    datasource_version: Optional[str]
) -> Tuple[MetricDefinitionData, List[MetricDataPoint]]:
    """
    Parse an OTLP metric and its data points.

    Args:
        metric: OTLP metric object
        resource_hash: Hash of the resource
        datasource_name: Name of the datasource
        datasource_version: Version of the datasource

    Returns:
        Tuple of (MetricDefinitionData, list of MetricDataPoints)
    """
    metric_name = metric.get('name', 'unknown')
    unit = metric.get('unit')
    description = metric.get('description')

    # Determine metric type and extract data points
    data_points = []
    metric_type = 'unknown'

    if 'gauge' in metric:
        metric_type = 'gauge'
        data_points_raw = metric['gauge'].get('dataPoints', [])
    elif 'sum' in metric:
        metric_type = 'sum'
        data_points_raw = metric['sum'].get('dataPoints', [])
    elif 'histogram' in metric:
        metric_type = 'histogram'
        data_points_raw = metric['histogram'].get('dataPoints', [])
    elif 'summary' in metric:
        metric_type = 'summary'
        data_points_raw = metric['summary'].get('dataPoints', [])
    elif 'exponentialHistogram' in metric:
        metric_type = 'exponentialHistogram'
        data_points_raw = metric['exponentialHistogram'].get('dataPoints', [])
    else:
        # Unknown metric type
        data_points_raw = []

    # Parse data points
    for dp in data_points_raw:
        data_points.append(parse_data_point(
            dp,
            resource_hash,
            datasource_name,
            datasource_version,
            metric_name
        ))

    # Create metric definition
    metric_def = MetricDefinitionData(
        datasource_name=datasource_name,
        datasource_version=datasource_version,
        name=metric_name,
        unit=unit,
        metric_type=metric_type,
        description=description
    )

    return metric_def, data_points


def parse_scope_metrics(
    scope_metrics: Dict[str, Any],
    resource_hash: str
) -> Tuple[DatasourceData, List[MetricDefinitionData], List[MetricDataPoint]]:
    """
    Parse OTLP scopeMetrics (datasource and its metrics).

    Args:
        scope_metrics: OTLP scopeMetrics object
        resource_hash: Hash of the resource

    Returns:
        Tuple of (DatasourceData, list of MetricDefinitionData, list of MetricDataPoints)
    """
    # Extract scope (datasource) information
    scope = scope_metrics.get('scope', {})
    datasource_name = scope.get('name', 'unknown')
    datasource_version = scope.get('version')

    datasource = DatasourceData(
        name=datasource_name,
        version=datasource_version
    )

    # Parse all metrics in this scope
    all_metric_defs = []
    all_data_points = []

    for metric in scope_metrics.get('metrics', []):
        metric_def, data_points = parse_metric(
            metric,
            resource_hash,
            datasource_name,
            datasource_version
        )
        all_metric_defs.append(metric_def)
        all_data_points.extend(data_points)

    return datasource, all_metric_defs, all_data_points


def parse_resource_metrics(
    resource_metrics: Dict[str, Any]
) -> Tuple[ResourceData, List[DatasourceData], List[MetricDefinitionData], List[MetricDataPoint]]:
    """
    Parse OTLP resourceMetrics (resource and all its metrics).

    Args:
        resource_metrics: OTLP resourceMetrics object

    Returns:
        Tuple of (ResourceData, list of DatasourceData, list of MetricDefinitionData, list of MetricDataPoints)
    """
    # Parse resource
    resource = resource_metrics.get('resource', {})
    attributes = parse_resource_attributes(resource)
    resource_hash = compute_resource_hash(attributes)

    resource_data = ResourceData(
        resource_hash=resource_hash,
        attributes=attributes
    )

    # Parse all scope metrics
    all_datasources = []
    all_metric_defs = []
    all_data_points = []

    for scope_metrics in resource_metrics.get('scopeMetrics', []):
        datasource, metric_defs, data_points = parse_scope_metrics(
            scope_metrics,
            resource_hash
        )
        all_datasources.append(datasource)
        all_metric_defs.extend(metric_defs)
        all_data_points.extend(data_points)

    return resource_data, all_datasources, all_metric_defs, all_data_points


def parse_otlp(otlp_payload: Dict[str, Any]) -> ParsedOTLP:
    """
    Parse complete OTLP payload into structured data.

    This is the main entry point for parsing OTLP JSON.

    Args:
        otlp_payload: Complete OTLP JSON payload as dictionary

    Returns:
        ParsedOTLP object with all extracted data

    Raises:
        ValueError: If payload is invalid or missing required fields
    """
    if not otlp_payload:
        raise ValueError("OTLP payload cannot be empty")

    if 'resourceMetrics' not in otlp_payload:
        raise ValueError("OTLP payload missing 'resourceMetrics' field")

    all_resources = []
    all_datasources = []
    all_metric_defs = []
    all_data_points = []

    # Parse each resourceMetrics entry
    for resource_metrics in otlp_payload.get('resourceMetrics', []):
        resource, datasources, metric_defs, data_points = parse_resource_metrics(resource_metrics)

        all_resources.append(resource)
        all_datasources.extend(datasources)
        all_metric_defs.extend(metric_defs)
        all_data_points.extend(data_points)

    return ParsedOTLP(
        resources=all_resources,
        datasources=all_datasources,
        metric_definitions=all_metric_defs,
        metric_data=all_data_points
    )


def deduplicate_resources(resources: List[ResourceData]) -> List[ResourceData]:
    """
    Deduplicate resources by hash.

    Args:
        resources: List of ResourceData objects

    Returns:
        Deduplicated list of ResourceData
    """
    seen_hashes = set()
    unique_resources = []

    for resource in resources:
        if resource.resource_hash not in seen_hashes:
            seen_hashes.add(resource.resource_hash)
            unique_resources.append(resource)

    return unique_resources


def deduplicate_datasources(datasources: List[DatasourceData]) -> List[DatasourceData]:
    """
    Deduplicate datasources by (name, version).

    Args:
        datasources: List of DatasourceData objects

    Returns:
        Deduplicated list of DatasourceData
    """
    seen_datasources = set()
    unique_datasources = []

    for datasource in datasources:
        key = (datasource.name, datasource.version)
        if key not in seen_datasources:
            seen_datasources.add(key)
            unique_datasources.append(datasource)

    return unique_datasources


def deduplicate_metric_definitions(metric_defs: List[MetricDefinitionData]) -> List[MetricDefinitionData]:
    """
    Deduplicate metric definitions by (datasource_name, datasource_version, name).

    Args:
        metric_defs: List of MetricDefinitionData objects

    Returns:
        Deduplicated list of MetricDefinitionData
    """
    seen_metrics = set()
    unique_metrics = []

    for metric_def in metric_defs:
        key = (metric_def.datasource_name, metric_def.datasource_version, metric_def.name)
        if key not in seen_metrics:
            seen_metrics.add(key)
            unique_metrics.append(metric_def)

    return unique_metrics
