# OTLP Parser Documentation

## Overview

The OTLP (OpenTelemetry Protocol) parser extracts and normalizes LogicMonitor metrics data from OTLP JSON payloads into structured data ready for database insertion.

## Features

 **Pure Functions** - No side effects, easy to test
 **Type Safety** - Uses dataclasses for structured output
 **Complete Parsing** - Handles resources, datasources, metrics, and time-series data
 **Flexible** - Supports all OTLP metric types (gauge, sum, histogram, etc.)
 **Timestamp Conversion** - Converts nanosecond timestamps to Python datetime
 **Deduplication** - Built-in deduplication helpers
 **Error Handling** - Validates input and provides clear error messages

## Quick Start

### Basic Usage

```python
from src.otlp_parser import parse_otlp
import json

# Load OTLP payload
with open('sample_otlp.json') as f:
    otlp_payload = json.load(f)

# Parse the payload
result = parse_otlp(otlp_payload)

# Access parsed data
print(f"Found {len(result.resources)} resources")
print(f"Found {len(result.datasources)} datasources")
print(f"Found {len(result.metric_definitions)} metric definitions")
print(f"Found {len(result.metric_data)} data points")
```

### Output Structure

The `parse_otlp()` function returns a `ParsedOTLP` object with:

```python
@dataclass
class ParsedOTLP:
    resources: List[ResourceData]
    datasources: List[DatasourceData]
    metric_definitions: List[MetricDefinitionData]
    metric_data: List[MetricDataPoint]
```

## Data Structures

### ResourceData

```python
@dataclass
class ResourceData:
    resource_hash: str          # SHA256 hash for deduplication
    attributes: Dict[str, Any]  # Full resource attributes
```

**Example:**
```python
ResourceData(
    resource_hash="a3f2...",
    attributes={
        "service.name": "web-server",
        "host.name": "server01",
        "environment": "production"
    }
)
```

### DatasourceData

```python
@dataclass
class DatasourceData:
    name: str                   # Datasource name (e.g., "CPU_Usage")
    version: Optional[str]      # Datasource version
```

**Example:**
```python
DatasourceData(
    name="CPU_Usage",
    version="1.0"
)
```

### MetricDefinitionData

```python
@dataclass
class MetricDefinitionData:
    datasource_name: str
    datasource_version: Optional[str]
    name: str                   # Metric name (e.g., "cpu.usage")
    unit: Optional[str]         # Unit (e.g., "percent", "bytes")
    metric_type: str            # Type: gauge, sum, histogram, etc.
    description: Optional[str]
```

**Example:**
```python
MetricDefinitionData(
    datasource_name="CPU_Usage",
    datasource_version="1.0",
    name="cpu.usage",
    unit="percent",
    metric_type="gauge",
    description="CPU usage percentage"
)
```

### MetricDataPoint

```python
@dataclass
class MetricDataPoint:
    resource_hash: str
    datasource_name: str
    datasource_version: Optional[str]
    metric_name: str
    timestamp: datetime         # Converted from nanoseconds
    value_double: Optional[float]
    value_int: Optional[int]
    attributes: Optional[Dict[str, Any]]
```

**Example:**
```python
MetricDataPoint(
    resource_hash="a3f2...",
    datasource_name="CPU_Usage",
    datasource_version="1.0",
    metric_name="cpu.usage",
    timestamp=datetime(2023, 11, 4, 12, 30, 56, tzinfo=timezone.utc),
    value_double=45.2,
    value_int=None,
    attributes=None
)
```

## OTLP Format

### Input Structure

The parser expects OTLP JSON in this format:

```json
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "web-server"}},
        {"key": "host.name", "value": {"stringValue": "server01"}}
      ]
    },
    "scopeMetrics": [{
      "scope": {
        "name": "CPU_Usage",
        "version": "1.0"
      },
      "metrics": [{
        "name": "cpu.usage",
        "unit": "percent",
        "description": "CPU usage",
        "gauge": {
          "dataPoints": [{
            "timeUnixNano": 1699123456000000000,
            "asDouble": 45.2
          }]
        }
      }]
    }]
  }]
}
```

## Supported Metric Types

The parser supports all OTLP metric types:

-  **gauge** - Point-in-time measurements
-  **sum** - Cumulative or delta sums
-  **histogram** - Distribution of values
-  **summary** - Quantile summaries
-  **exponentialHistogram** - Exponentially-bucketed histograms

## Functions

### Main Parser

#### `parse_otlp(otlp_payload: Dict[str, Any]) -> ParsedOTLP`

Main entry point for parsing OTLP data.

**Parameters:**
- `otlp_payload`: Complete OTLP JSON as dictionary

**Returns:**
- `ParsedOTLP` object with all extracted data

**Raises:**
- `ValueError`: If payload is invalid or missing required fields

**Example:**
```python
result = parse_otlp(otlp_payload)
```

### Resource Functions

#### `parse_resource_attributes(resource: Dict[str, Any]) -> Dict[str, Any]`

Extract resource attributes into a flat dictionary.

#### `compute_resource_hash(attributes: Dict[str, Any]) -> str`

Compute SHA256 hash of resource attributes for deduplication.

**Note:** Hash is order-independent (uses sorted JSON).

### Timestamp Functions

#### `convert_nano_timestamp(time_unix_nano: int) -> datetime`

Convert OTLP nanosecond timestamp to Python datetime.

**Example:**
```python
timestamp = convert_nano_timestamp(1699123456000000000)
# datetime(2023, 11, 4, 12, 30, 56, tzinfo=timezone.utc)
```

### Deduplication Functions

#### `deduplicate_resources(resources: List[ResourceData]) -> List[ResourceData]`

Remove duplicate resources by hash.

#### `deduplicate_datasources(datasources: List[DatasourceData]) -> List[DatasourceData]`

Remove duplicate datasources by (name, version).

#### `deduplicate_metric_definitions(metric_defs: List[MetricDefinitionData]) -> List[MetricDefinitionData]`

Remove duplicate metric definitions by (datasource_name, datasource_version, name).

## Database Integration Example

### Insert Parsed Data into Database

```python
import json
from src.otlp_parser import parse_otlp, deduplicate_resources, deduplicate_datasources, deduplicate_metric_definitions

# Parse OTLP payload
with open('otlp_payload.json') as f:
    payload = json.load(f)

result = parse_otlp(payload)

# Deduplicate before insertion
unique_resources = deduplicate_resources(result.resources)
unique_datasources = deduplicate_datasources(result.datasources)
unique_metric_defs = deduplicate_metric_definitions(result.metric_definitions)

# Insert into database
with db_connection.cursor() as cur:
    # 1. Insert resources
    for resource in unique_resources:
        cur.execute("""
            INSERT INTO resources (resource_hash, attributes)
            VALUES (%s, %s)
            ON CONFLICT (resource_hash) DO UPDATE SET updated_at = NOW()
        """, (resource.resource_hash, json.dumps(resource.attributes)))

    # 2. Insert datasources
    for datasource in unique_datasources:
        cur.execute("""
            INSERT INTO datasources (name, version)
            VALUES (%s, %s)
            ON CONFLICT (name, version) DO NOTHING
        """, (datasource.name, datasource.version))

    # 3. Insert metric definitions
    for metric_def in unique_metric_defs:
        # First get datasource_id
        cur.execute("""
            SELECT id FROM datasources WHERE name = %s AND version = %s
        """, (metric_def.datasource_name, metric_def.datasource_version))
        datasource_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (datasource_id, name) DO NOTHING
        """, (datasource_id, metric_def.name, metric_def.unit, metric_def.metric_type, metric_def.description))

    # 4. Insert metric data points
    for data_point in result.metric_data:
        # Get resource_id
        cur.execute("""
            SELECT id FROM resources WHERE resource_hash = %s
        """, (data_point.resource_hash,))
        resource_id = cur.fetchone()[0]

        # Get metric_definition_id
        cur.execute("""
            SELECT md.id FROM metric_definitions md
            JOIN datasources ds ON md.datasource_id = ds.id
            WHERE ds.name = %s AND ds.version = %s AND md.name = %s
        """, (data_point.datasource_name, data_point.datasource_version, data_point.metric_name))
        metric_def_id = cur.fetchone()[0]

        # Insert data point
        cur.execute("""
            INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double, value_int, attributes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (resource_id, metric_def_id, data_point.timestamp, data_point.value_double, data_point.value_int,
              json.dumps(data_point.attributes) if data_point.attributes else None))

    db_connection.commit()
```

## Testing

### Run Tests

```bash
# Run all parser tests
uv run pytest tests/test_otlp_parser.py -v

# Run specific test
uv run pytest tests/test_otlp_parser.py::test_parse_otlp_with_sample_data -v

# Run with coverage
uv run pytest tests/test_otlp_parser.py --cov=src.otlp_parser
```

### Test Results

```
30 passed in 0.07s
```

Test coverage includes:
-  Attribute value extraction (string, int, double, bool)
-  Resource attribute parsing
-  Resource hash computation and consistency
-  Timestamp conversion with precision
-  Data point parsing (double and int values)
-  Metric parsing (gauge, sum, unknown types)
-  Scope metrics parsing
-  Complete OTLP payload parsing
-  Deduplication functions
-  Error handling
-  Multiple resources and metrics

## Error Handling

The parser validates input and provides clear error messages:

```python
# Empty payload
try:
    parse_otlp({})
except ValueError as e:
    print(e)  # "OTLP payload cannot be empty"

# Missing resourceMetrics
try:
    parse_otlp({"otherField": "value"})
except ValueError as e:
    print(e)  # "OTLP payload missing 'resourceMetrics' field"
```

## Performance Considerations

- **Pure Functions**: All functions are pure (no side effects)
- **Efficient Hashing**: SHA256 hashing for resource deduplication
- **Lazy Evaluation**: Only parses what's in the payload
- **Memory Efficient**: Returns structured data, not storing intermediate results

## Best Practices

1. **Deduplicate Before Database Insertion**
   ```python
   unique_resources = deduplicate_resources(result.resources)
   ```

2. **Handle Errors Gracefully**
   ```python
   try:
       result = parse_otlp(payload)
   except ValueError as e:
       logger.error(f"Invalid OTLP payload: {e}")
       return
   ```

3. **Validate Timestamps**
   ```python
   for data_point in result.metric_data:
       assert data_point.timestamp.tzinfo == timezone.utc
   ```

4. **Use ON CONFLICT for Upserts**
   ```python
   INSERT INTO resources (resource_hash, attributes)
   VALUES (%s, %s)
   ON CONFLICT (resource_hash) DO UPDATE SET updated_at = NOW()
   ```

## Extending the Parser

### Adding New Metric Types

To support new OTLP metric types:

```python
def parse_metric(metric, resource_hash, datasource_name, datasource_version):
    # ... existing code ...

    elif 'newMetricType' in metric:
        metric_type = 'newMetricType'
        data_points_raw = metric['newMetricType'].get('dataPoints', [])

    # ... rest of code ...
```

### Custom Attribute Extraction

For custom attribute types:

```python
def extract_attribute_value(attr_value):
    # ... existing types ...

    elif 'customType' in attr_value:
        return process_custom_type(attr_value['customType'])
```

## Troubleshooting

### Issue: Timestamps are None

**Cause:** Missing `timeUnixNano` in data points

**Solution:** Check OTLP payload has valid timestamps

### Issue: Resource hash mismatch

**Cause:** Attribute order affecting hash

**Solution:** The parser automatically sorts attributes for consistent hashing

### Issue: Missing metric data

**Cause:** Unsupported metric type

**Solution:** Check metric type is supported (gauge, sum, etc.)

## References

- [OpenTelemetry Protocol Specification](https://opentelemetry.io/docs/specs/otlp/)
- [OTLP Metrics Data Model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/)
- [LogicMonitor OTLP Integration](https://www.logicmonitor.com/)
