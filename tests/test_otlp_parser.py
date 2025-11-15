# ABOUTME: Comprehensive tests for OTLP parser module
# ABOUTME: Validates parsing of resources, datasources, metrics, and time-series data

import pytest
import json
from datetime import datetime, timezone
from pathlib import Path

from src.otlp_parser import (
    parse_otlp,
    parse_resource_attributes,
    compute_resource_hash,
    convert_nano_timestamp,
    extract_attribute_value,
    parse_data_point,
    parse_metric,
    parse_scope_metrics,
    parse_resource_metrics,
    deduplicate_resources,
    deduplicate_datasources,
    deduplicate_metric_definitions,
    ResourceData,
    DatasourceData,
    MetricDefinitionData,
    MetricDataPoint,
    ParsedOTLP
)


def test_extract_attribute_value_string():
    """Test extracting string value from OTLP attribute."""
    attr_value = {"stringValue": "web-server"}
    result = extract_attribute_value(attr_value)
    assert result == "web-server"


def test_extract_attribute_value_int():
    """Test extracting integer value from OTLP attribute."""
    attr_value = {"intValue": 42}
    result = extract_attribute_value(attr_value)
    assert result == 42


def test_extract_attribute_value_double():
    """Test extracting double value from OTLP attribute."""
    attr_value = {"doubleValue": 3.14}
    result = extract_attribute_value(attr_value)
    assert result == 3.14


def test_extract_attribute_value_bool():
    """Test extracting boolean value from OTLP attribute."""
    attr_value = {"boolValue": True}
    result = extract_attribute_value(attr_value)
    assert result is True


def test_parse_resource_attributes():
    """Test parsing OTLP resource attributes."""
    resource = {
        "attributes": [
            {"key": "service.name", "value": {"stringValue": "web-server"}},
            {"key": "host.name", "value": {"stringValue": "server01"}},
            {"key": "port", "value": {"intValue": 8080}}
        ]
    }

    attributes = parse_resource_attributes(resource)

    assert attributes["service.name"] == "web-server"
    assert attributes["host.name"] == "server01"
    assert attributes["port"] == 8080


def test_parse_resource_attributes_empty():
    """Test parsing resource with no attributes."""
    resource = {"attributes": []}
    attributes = parse_resource_attributes(resource)
    assert attributes == {}


def test_compute_resource_hash():
    """Test computing consistent resource hash."""
    attrs1 = {"service": "web", "host": "server01"}
    attrs2 = {"host": "server01", "service": "web"}  # Different order

    hash1 = compute_resource_hash(attrs1)
    hash2 = compute_resource_hash(attrs2)

    # Hashes should be identical (order-independent)
    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 produces 64 hex characters


def test_compute_resource_hash_different():
    """Test that different attributes produce different hashes."""
    attrs1 = {"service": "web", "host": "server01"}
    attrs2 = {"service": "api", "host": "server02"}

    hash1 = compute_resource_hash(attrs1)
    hash2 = compute_resource_hash(attrs2)

    assert hash1 != hash2


def test_convert_nano_timestamp():
    """Test converting nanosecond timestamp to datetime."""
    # 1699123456000000000 nanoseconds
    time_unix_nano = 1699123456000000000
    dt = convert_nano_timestamp(time_unix_nano)

    assert isinstance(dt, datetime)
    assert dt.tzinfo == timezone.utc
    # Verify timestamp is approximately correct (Nov 2023)
    assert dt.year == 2023
    assert dt.month == 11


def test_parse_data_point_with_double():
    """Test parsing data point with asDouble value."""
    data_point = {
        "timeUnixNano": 1699123456000000000,
        "asDouble": 45.2
    }

    result = parse_data_point(
        data_point,
        "test_hash_123",
        "CPU_Usage",
        "1.0",
        "cpu.usage"
    )

    assert result.resource_hash == "test_hash_123"
    assert result.datasource_name == "CPU_Usage"
    assert result.datasource_version == "1.0"
    assert result.metric_name == "cpu.usage"
    assert result.value_double == 45.2
    assert result.value_int is None
    assert isinstance(result.timestamp, datetime)


def test_parse_data_point_with_int():
    """Test parsing data point with asInt value."""
    data_point = {
        "timeUnixNano": 1699123456000000000,
        "asInt": 1024
    }

    result = parse_data_point(
        data_point,
        "test_hash_456",
        "Memory",
        None,
        "memory.bytes"
    )

    assert result.value_int == 1024
    assert result.value_double is None


def test_parse_data_point_with_attributes():
    """Test parsing data point with additional attributes."""
    data_point = {
        "timeUnixNano": 1699123456000000000,
        "asDouble": 100.0,
        "attributes": [
            {"key": "unit", "value": {"stringValue": "percent"}},
            {"key": "threshold", "value": {"intValue": 90}}
        ]
    }

    result = parse_data_point(
        data_point,
        "test_hash",
        "TestDS",
        "1.0",
        "test.metric"
    )

    assert result.attributes is not None
    assert result.attributes["unit"] == "percent"
    assert result.attributes["threshold"] == 90


def test_parse_metric_gauge():
    """Test parsing a gauge metric."""
    metric = {
        "name": "cpu.usage",
        "unit": "percent",
        "description": "CPU usage percentage",
        "gauge": {
            "dataPoints": [
                {"timeUnixNano": 1699123456000000000, "asDouble": 45.2},
                {"timeUnixNano": 1699123457000000000, "asDouble": 46.1}
            ]
        }
    }

    metric_def, data_points = parse_metric(
        metric,
        "resource_hash_123",
        "CPU_Usage",
        "1.0"
    )

    assert metric_def.name == "cpu.usage"
    assert metric_def.unit == "percent"
    assert metric_def.metric_type == "gauge"
    assert metric_def.description == "CPU usage percentage"

    assert len(data_points) == 2
    assert data_points[0].value_double == 45.2
    assert data_points[1].value_double == 46.1


def test_parse_metric_sum():
    """Test parsing a sum metric."""
    metric = {
        "name": "requests.total",
        "unit": "count",
        "sum": {
            "dataPoints": [
                {"timeUnixNano": 1699123456000000000, "asInt": 1000}
            ]
        }
    }

    metric_def, data_points = parse_metric(
        metric,
        "resource_hash_456",
        "HTTP_Requests",
        "2.0"
    )

    assert metric_def.metric_type == "sum"
    assert len(data_points) == 1
    assert data_points[0].value_int == 1000


def test_parse_scope_metrics():
    """Test parsing scopeMetrics."""
    scope_metrics = {
        "scope": {
            "name": "CPU_Usage",
            "version": "1.0"
        },
        "metrics": [
            {
                "name": "cpu.usage",
                "unit": "percent",
                "gauge": {
                    "dataPoints": [
                        {"timeUnixNano": 1699123456000000000, "asDouble": 45.2}
                    ]
                }
            }
        ]
    }

    datasource, metric_defs, data_points = parse_scope_metrics(
        scope_metrics,
        "resource_hash_789"
    )

    assert datasource.name == "CPU_Usage"
    assert datasource.version == "1.0"
    assert len(metric_defs) == 1
    assert len(data_points) == 1


def test_parse_resource_metrics():
    """Test parsing complete resourceMetrics."""
    resource_metrics = {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "web-server"}},
                {"key": "host.name", "value": {"stringValue": "server01"}}
            ]
        },
        "scopeMetrics": [
            {
                "scope": {"name": "CPU_Usage", "version": "1.0"},
                "metrics": [
                    {
                        "name": "cpu.usage",
                        "unit": "percent",
                        "gauge": {
                            "dataPoints": [
                                {"timeUnixNano": 1699123456000000000, "asDouble": 45.2}
                            ]
                        }
                    }
                ]
            }
        ]
    }

    resource, datasources, metric_defs, data_points = parse_resource_metrics(resource_metrics)

    assert resource.attributes["service.name"] == "web-server"
    assert resource.attributes["host.name"] == "server01"
    assert len(datasources) == 1
    assert datasources[0].name == "CPU_Usage"
    assert len(metric_defs) == 1
    assert len(data_points) == 1


def test_parse_otlp_with_sample_data(sample_otlp_cpu_metrics):
    """Test parsing complete OTLP payload with sample CPU metrics."""
    result = parse_otlp(sample_otlp_cpu_metrics)

    assert isinstance(result, ParsedOTLP)
    assert len(result.resources) == 1
    assert len(result.datasources) == 1
    assert len(result.metric_definitions) == 1
    assert len(result.metric_data) == 1

    # Check resource
    resource = result.resources[0]
    assert resource.attributes["service.name"] == "web-server"
    assert resource.attributes["host.name"] == "server01"

    # Check datasource
    datasource = result.datasources[0]
    assert datasource.name == "CPU_Usage"
    assert datasource.version == "1.0"

    # Check metric definition
    metric_def = result.metric_definitions[0]
    assert metric_def.name == "cpu.usage"
    assert metric_def.unit == "percent"
    assert metric_def.metric_type == "gauge"

    # Check data point
    data_point = result.metric_data[0]
    assert data_point.value_double == 45.2
    assert isinstance(data_point.timestamp, datetime)


def test_parse_otlp_with_memory_metrics(sample_otlp_memory_metrics):
    """Test parsing OTLP payload with memory metrics."""
    result = parse_otlp(sample_otlp_memory_metrics)

    assert len(result.resources) == 1
    assert len(result.datasources) == 1
    assert len(result.metric_definitions) == 1
    assert len(result.metric_data) == 1

    # Check memory-specific data
    metric_def = result.metric_definitions[0]
    assert metric_def.name == "memory.usage"
    assert metric_def.unit == "bytes"

    data_point = result.metric_data[0]
    assert data_point.value_int == 8589934592  # 8GB in bytes
    assert data_point.value_double is None


def test_parse_otlp_with_multi_metric(sample_otlp_multi_metric):
    """Test parsing OTLP payload with multiple resources and metrics."""
    result = parse_otlp(sample_otlp_multi_metric)

    # Should have 2 resources
    assert len(result.resources) == 2

    # Should have multiple datasources
    assert len(result.datasources) >= 2

    # Should have multiple metric definitions
    assert len(result.metric_definitions) >= 3

    # Should have multiple data points
    assert len(result.metric_data) >= 3

    # Verify different resource hashes
    resource_hashes = [r.resource_hash for r in result.resources]
    assert len(set(resource_hashes)) == 2, "Should have 2 unique resource hashes"


def test_parse_otlp_empty_payload():
    """Test parsing empty OTLP payload raises error."""
    with pytest.raises(ValueError, match="cannot be empty"):
        parse_otlp({})


def test_parse_otlp_missing_resource_metrics():
    """Test parsing OTLP without resourceMetrics raises error."""
    payload = {"otherField": "value"}

    with pytest.raises(ValueError, match="missing 'resourceMetrics'"):
        parse_otlp(payload)


def test_parse_otlp_to_dict(sample_otlp_cpu_metrics):
    """Test converting ParsedOTLP to dictionary."""
    result = parse_otlp(sample_otlp_cpu_metrics)
    result_dict = result.to_dict()

    assert 'resources' in result_dict
    assert 'datasources' in result_dict
    assert 'metric_definitions' in result_dict
    assert 'metric_data' in result_dict

    # Verify structure is serializable
    json_str = json.dumps(result_dict, default=str)  # datetime needs str conversion
    assert len(json_str) > 0


def test_deduplicate_resources():
    """Test deduplicating resources by hash."""
    resources = [
        ResourceData("hash1", {"service": "web"}),
        ResourceData("hash1", {"service": "web"}),  # Duplicate
        ResourceData("hash2", {"service": "api"})
    ]

    unique = deduplicate_resources(resources)

    assert len(unique) == 2
    assert unique[0].resource_hash == "hash1"
    assert unique[1].resource_hash == "hash2"


def test_deduplicate_datasources():
    """Test deduplicating datasources by name and version."""
    datasources = [
        DatasourceData("CPU_Usage", "1.0"),
        DatasourceData("CPU_Usage", "1.0"),  # Duplicate
        DatasourceData("CPU_Usage", "2.0"),  # Different version
        DatasourceData("Memory", "1.0")
    ]

    unique = deduplicate_datasources(datasources)

    assert len(unique) == 3
    # Should have: CPU_Usage v1.0, CPU_Usage v2.0, Memory v1.0


def test_deduplicate_metric_definitions():
    """Test deduplicating metric definitions."""
    metrics = [
        MetricDefinitionData("CPU_Usage", "1.0", "cpu.usage", "percent", "gauge", None),
        MetricDefinitionData("CPU_Usage", "1.0", "cpu.usage", "percent", "gauge", None),  # Duplicate
        MetricDefinitionData("CPU_Usage", "1.0", "cpu.idle", "percent", "gauge", None),  # Different metric
        MetricDefinitionData("Memory", "1.0", "memory.used", "bytes", "gauge", None)
    ]

    unique = deduplicate_metric_definitions(metrics)

    assert len(unique) == 3


def test_parse_otlp_from_file(sample_otlp_data):
    """Test parsing OTLP data loaded from fixture file."""
    result = parse_otlp(sample_otlp_data)

    assert len(result.resources) > 0
    assert len(result.datasources) > 0
    assert len(result.metric_definitions) > 0
    assert len(result.metric_data) > 0


def test_resource_hash_consistency():
    """Test that resource hashes are consistent across parses."""
    payload1 = {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "test"}},
                    {"key": "host.name", "value": {"stringValue": "host1"}}
                ]
            },
            "scopeMetrics": []
        }]
    }

    payload2 = {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "host.name", "value": {"stringValue": "host1"}},  # Different order
                    {"key": "service.name", "value": {"stringValue": "test"}}
                ]
            },
            "scopeMetrics": []
        }]
    }

    result1 = parse_otlp(payload1)
    result2 = parse_otlp(payload2)

    assert result1.resources[0].resource_hash == result2.resources[0].resource_hash


def test_timestamp_precision():
    """Test that nanosecond timestamps are converted with precision."""
    # Specific nanosecond timestamp
    time_unix_nano = 1699123456789000000  # Note the 789 milliseconds

    dt = convert_nano_timestamp(time_unix_nano)

    # Verify microsecond precision is preserved
    assert dt.microsecond == 789000


def test_parse_metric_unknown_type():
    """Test parsing metric with unknown type."""
    metric = {
        "name": "unknown.metric",
        "unknownType": {}  # Not a standard OTLP metric type
    }

    metric_def, data_points = parse_metric(
        metric,
        "hash",
        "Unknown",
        "1.0"
    )

    assert metric_def.metric_type == "unknown"
    assert len(data_points) == 0


def test_multiple_scope_metrics_per_resource():
    """Test parsing resource with multiple scopeMetrics."""
    resource_metrics = {
        "resource": {
            "attributes": [
                {"key": "host", "value": {"stringValue": "server01"}}
            ]
        },
        "scopeMetrics": [
            {
                "scope": {"name": "CPU", "version": "1.0"},
                "metrics": [
                    {
                        "name": "cpu.usage",
                        "gauge": {
                            "dataPoints": [
                                {"timeUnixNano": 1699123456000000000, "asDouble": 45.2}
                            ]
                        }
                    }
                ]
            },
            {
                "scope": {"name": "Memory", "version": "1.0"},
                "metrics": [
                    {
                        "name": "memory.usage",
                        "gauge": {
                            "dataPoints": [
                                {"timeUnixNano": 1699123456000000000, "asInt": 1024}
                            ]
                        }
                    }
                ]
            }
        ]
    }

    resource, datasources, metric_defs, data_points = parse_resource_metrics(resource_metrics)

    assert len(datasources) == 2
    assert len(metric_defs) == 2
    assert len(data_points) == 2

    # Verify both datasources
    ds_names = [ds.name for ds in datasources]
    assert "CPU" in ds_names
    assert "Memory" in ds_names
