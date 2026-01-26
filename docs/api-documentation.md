# Description: API reference for LogicMonitor OTLP Data Pipeline endpoints.
# Description: Covers ingestion, health, ML endpoints, and data export APIs.

# LogicMonitor Data Pipeline - API Documentation

**Version:** 32.0
**Base URL:** `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io`
**Protocol:** HTTPS
**Storage Mode:** Data Lake only (v32+)

---

## Table of Contents

1. [Authentication](#authentication)
2. [Data Ingestion](#data-ingestion)
3. [Health & Monitoring](#health--monitoring)
4. [ML Data Service](#ml-data-service)
5. [Data Export (Hot Cache)](#data-export-hot-cache)
6. [Error Codes](#error-codes)

---

## Authentication

### API Key Authentication

**Status:** NOT IMPLEMENTED

Endpoints are publicly accessible. Consider adding authentication for production use.

### Azure Managed Identity

Data Lake and Synapse connections use Azure managed identity. No credentials are stored.

---

## Data Ingestion

### POST /api/HttpIngest

Ingest OTLP (OpenTelemetry Protocol) formatted metrics data.

**Headers:**
```http
Content-Type: application/json
Content-Encoding: gzip (optional, recommended for >1KB)
```

**Request Body:**
```json
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "my-service"}},
          {"key": "host.name", "value": {"stringValue": "host-01"}}
        ]
      },
      "scopeMetrics": [
        {
          "scope": {"name": "instrumentation-scope", "version": "1.0.0"},
          "metrics": [
            {
              "name": "cpu.usage",
              "description": "CPU usage percentage",
              "unit": "percent",
              "gauge": {
                "dataPoints": [
                  {
                    "asDouble": 45.5,
                    "timeUnixNano": 1673000000000000000,
                    "attributes": []
                  }
                ]
              }
            }
          ]
        }
      ]
    }
  ]
}
```

**Response:** `200 OK`
```json
{
  "status": "success",
  "message": "Data ingested successfully",
  "buffered": {
    "metric_data": 150,
    "resources": 1,
    "datasources": 5,
    "metric_definitions": 25
  }
}
```

**Example (gzip):**
```bash
curl -X POST https://ca-cta-lm-ingest.../api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @payload.json.gz
```

---

## Health & Monitoring

### GET /api/health

Health check endpoint with component-level status.

**Response:** `200 OK` (healthy)
```json
{
  "status": "healthy",
  "timestamp": "2026-01-26T02:30:00Z",
  "version": "32.0.0",
  "mode": "datalake_only",
  "components": {
    "datalake": {
      "status": "healthy",
      "buffer": {
        "metric_data_buffered": 827,
        "resources_buffered": 1,
        "datasources_buffered": 33,
        "metric_definitions_buffered": 332,
        "flush_threshold": 10000
      }
    },
    "hot_cache": {
      "status": "disabled",
      "enabled": false
    },
    "synapse": {
      "status": "healthy",
      "server": "syn-lm-analytics-ondemand.sql.azuresynapse.net",
      "database": "master"
    },
    "background_tasks": {
      "running": 1,
      "total": 1,
      "tasks": ["datalake_flush"]
    }
  }
}
```

**Components:**
- `datalake`: Data Lake writer buffer status
- `hot_cache`: PostgreSQL hot cache status (disabled by default)
- `synapse`: Synapse Serverless SQL connection status
- `background_tasks`: Running background tasks

---

## ML Data Service

The ML Data Service provides endpoints for serving training data to Precursor ML models. These endpoints query Azure Data Lake Gen2 via Synapse Serverless SQL.

**Query Engine:** Azure Synapse Serverless SQL (~$5/TB scanned)

### GET /api/ml/profiles

List all available feature profiles with their numerical and categorical features.

**Response:** `200 OK`
```json
{
  "profiles": {
    "collector": {
      "description": "LogicMonitor Collector self-monitoring metrics",
      "numerical_features": ["ExecuteTime", "AvgExecTime", "MaxExecTime"],
      "categorical_features": ["Active", "santabaConnection"],
      "total_features": 38
    },
    "kubernetes": {
      "description": "Container orchestration workloads",
      "numerical_features": ["cpuUsageNanoCores", "memoryUsageBytes"],
      "categorical_features": ["podConditionPhase"],
      "total_features": 58
    }
  }
}
```

### GET /api/ml/inventory

Get inventory of available metrics, resources, and datasources from Data Lake.

**Query Parameters:**
- `datasource` (optional): Filter by datasource name

**Response:** `200 OK`
```json
{
  "metrics": [
    {
      "metric_name": "ExecuteTime",
      "datasource_name": "WinCollectorUsage",
      "data_points": 15420
    }
  ],
  "resources": [
    {
      "resource_hash": "abc123...",
      "data_points": 45670
    }
  ],
  "time_range": {
    "start": "2026-01-01T00:00:00Z",
    "end": "2026-01-26T02:30:00Z"
  },
  "total_data_points": 56000000,
  "source": "synapse_datalake"
}
```

### GET /api/ml/training-data

Extract training data for ML models with profile-based filtering.

**Query Parameters:**
- `profile` (optional): Feature profile name
- `start_time` (required): ISO 8601 timestamp
- `end_time` (required): ISO 8601 timestamp
- `metric_names` (optional): Comma-separated metric names
- `resource_hash` (optional): Filter by resource hash
- `limit` (optional): Maximum records (default: 10000)
- `offset` (optional): Pagination offset (default: 0)

**Example:**
```bash
curl "https://{host}/api/ml/training-data?start_time=2026-01-01T00:00:00Z&end_time=2026-01-25T00:00:00Z&limit=5000"
```

**Response:** `200 OK`
```json
{
  "data": [
    {
      "resource_hash": "abc123...",
      "datasource_name": "WinCollectorUsage",
      "metric_name": "ExecuteTime",
      "timestamp": "2026-01-24T12:00:00Z",
      "value": 42.5,
      "attributes": "{\"instance\": \"inst1\"}"
    }
  ],
  "meta": {
    "total": 125000,
    "limit": 5000,
    "offset": 0,
    "start_time": "2026-01-01T00:00:00Z",
    "end_time": "2026-01-25T00:00:00Z",
    "source": "synapse_datalake"
  }
}
```

### GET /api/ml/profile-coverage

Check which profile metrics are available in the Data Lake.

**Query Parameters:**
- `profile` (optional): Single profile to check

**Response:** `200 OK`
```json
{
  "profiles": [
    {
      "name": "collector",
      "description": "LogicMonitor Collector self-monitoring metrics",
      "total_expected": 38,
      "available_count": 38,
      "coverage_percent": 100.0,
      "available": ["ExecuteTime", "AvgExecTime"],
      "missing": []
    }
  ]
}
```

---

## Data Export (Hot Cache)

**Note:** Export endpoints require PostgreSQL hot cache to be enabled (`HOT_CACHE_ENABLED=true`). These are disabled by default in Data Lake only mode.

### GET /metrics

Prometheus format export.

**Status:** Requires hot cache

### GET /grafana/search, POST /grafana/query

Grafana SimpleJSON datasource.

**Status:** Requires hot cache

### GET /export/powerbi, /export/csv, /export/json

Export endpoints for BI tools.

**Status:** Requires hot cache

---

## Error Codes

| Code | Description | Resolution |
|------|-------------|------------|
| 400 | Bad Request | Check request format and parameters |
| 404 | Not Found | Verify endpoint URL |
| 500 | Internal Server Error | Check logs |
| 503 | Service Unavailable | Data Lake or Synapse unavailable |

**Error Response Format:**
```json
{
  "detail": "Error description"
}
```

---

## Best Practices

### Data Ingestion

1. **Use gzip compression** for payloads > 1KB
2. **Batch metrics** (100-1000 per request)
3. **Include timestamps** in Unix nanoseconds
4. **Set proper resource attributes** for filtering

### ML Training Data

1. **Use feature profiles** for domain-specific metrics
2. **Check profile coverage** before training
3. **Use time-bounded queries** to limit scan costs
4. **Paginate large queries** with limit/offset

### Cost Optimization

1. Synapse queries scan Parquet files (~$5/TB)
2. Use time-bounded queries to reduce scans
3. Filter by specific metrics when possible
4. Data Lake storage is cheaper than PostgreSQL

---

## Support

**Repository:** https://github.com/ryanmat/HttpIngest
**Health Check:** https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
