# Description: API reference for LogicMonitor OTLP Data Pipeline endpoints.
# Description: Covers ingestion, health, ML endpoints, and data export APIs.

# LogicMonitor Data Pipeline - API Documentation

**Version:** 49.0.0
**Base URL:** `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io`
**Protocol:** HTTPS
**Storage:** Azure Data Lake Gen2 (primary) + Azure Synapse Serverless SQL (query engine)
**Hot Cache:** PostgreSQL (dormant, for dashboarding if/when needed)

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
  "stats": {
    "datalake_written": 150,
    "hot_cache_written": 0,
    "errors": []
  },
  "timestamp": "2026-01-29T12:00:00Z"
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

### GET /health

Root health check for Azure Container Apps probes. Lightweight, no component checks.

**Response:** `200 OK`
```json
{
  "status": "healthy",
  "timestamp": "2026-01-29T23:31:19Z",
  "version": "49.0.0"
}
```

### GET /api/health

Detailed health check endpoint with component-level status.

**Response:** `200 OK` (healthy)
```json
{
  "status": "healthy",
  "timestamp": "2026-01-29T23:31:19Z",
  "version": "49.0.0",
  "mode": "datalake_only",
  "components": {
    "datalake": {
      "status": "healthy",
      "buffer": {
        "metric_data_buffered": 1319,
        "resources_buffered": 80,
        "datasources_buffered": 31,
        "metric_definitions_buffered": 374,
        "flush_threshold": 10000
      }
    },
    "hot_cache": {
      "status": "disabled",
      "enabled": false
    },
    "ingestion_router": {
      "config": {
        "write_to_datalake": true,
        "write_to_hot_cache": false
      }
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
- `hot_cache`: PostgreSQL hot cache status (dormant by default, for dashboarding)
- `ingestion_router`: Routing config showing active storage backends
- `synapse`: Synapse Serverless SQL connection status (query engine for Precursor)
- `background_tasks`: Running background tasks

### GET /metrics

Prometheus-format metrics using in-memory counters. No database dependency.

**Response:** `200 OK` (text/plain)
```
httpingest_requests_total 40028
httpingest_requests_success_total 40028
httpingest_requests_error_total 0
httpingest_metrics_ingested_total 1259335
httpingest_datalake_flushes_total 185
httpingest_datalake_records_written_total 1006331
httpingest_datalake_buffer_size 1539
httpingest_info{version="49.0.0",mode="datalake_only"} 1
```

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
  "total_data_points": 56000000
}
```

### GET /api/ml/training-data

Extract training data for ML models with profile-based filtering.

**Query Parameters:**
- `profile` (optional): Feature profile name
- `start_time` (optional): ISO 8601 timestamp (default: 7 days ago)
- `end_time` (optional): ISO 8601 timestamp (default: now)
- `resource_id` (optional): Filter by resource ID (integer)
- `limit` (optional): Maximum records (default: 10000, max: 100000)
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
      "resource_hash": "5cc48976a64a...",
      "datasource_name": "LogicMonitor_Collector_ThreadUsage",
      "metric_name": "ThreadCount",
      "timestamp": "2026-01-28T17:56:11.046000",
      "value_double": 40.0,
      "value_int": null,
      "attributes": "{\"dataSourceInstanceName\": \"...\", \"datapointid\": \"17189\"}",
      "ingested_at": "2026-01-28T17:56:11.819766",
      "value": 40.0
    }
  ],
  "meta": {
    "total": 125000,
    "limit": 5000,
    "offset": 0,
    "start_time": "2026-01-01T00:00:00Z",
    "end_time": "2026-01-25T00:00:00Z"
  }
}
```

**Note:** When querying via Synapse, response includes `value_double`, `value_int`, and computed `value` fields.
When querying via PostgreSQL hot cache, response includes `resource_id`, `host_name`, `service_name`, `datasource_instance` fields instead.

### GET /api/ml/profile-coverage

Check which profile metrics are available in the database.

**Status:** Requires hot cache (PostgreSQL). Returns 503 without it.

**Query Parameters:**
- `profile` (optional): Single profile to check

**Response:** `200 OK`
```json
{
  "profiles": [
    {
      "name": "collector",
      "description": "LogicMonitor Collector self-monitoring metrics",
      "total_expected": 47,
      "total_available": 47,
      "coverage_percent": 100.0,
      "available": ["ExecuteTime", "AvgExecTime"],
      "missing": []
    }
  ]
}
```

### GET /api/ml/quality

Assess data quality for ML training readiness.

**Status:** Requires hot cache (PostgreSQL). Returns 503 without it.

**Query Parameters:**
- `profile` (optional): Filter by profile name
- `hours` (optional): Lookback period, 1-168 (default: 24)

**Response:** `200 OK`
```json
{
  "freshness": [...],
  "gaps": [...],
  "ranges": [...],
  "summary": {"score": 85}
}
```

---

## Endpoint Availability by Mode

| Endpoint | Data Lake + Synapse | Requires Hot Cache |
|----------|--------------------|--------------------|
| `GET /health` | Works | No |
| `GET /api/health` | Works | No |
| `GET /metrics` | Works | No |
| `POST /api/HttpIngest` | Works | No |
| `GET /api/ml/profiles` | Works | No |
| `GET /api/ml/inventory` | Works (via Synapse) | No |
| `GET /api/ml/training-data` | Works (via Synapse) | No |
| `GET /api/ml/profile-coverage` | Returns 503 | Yes |
| `GET /api/ml/quality` | Returns 503 | Yes |
| `GET /grafana/search` | Returns 503 | Yes |
| `POST /grafana/query` | Returns 503 | Yes |
| `GET /export/powerbi` | Returns 503 | Yes |
| `GET /export/csv` | Returns 503 | Yes |
| `GET /export/json` | Returns 503 | Yes |

---

## Data Export (Hot Cache)

**Note:** Export endpoints require PostgreSQL hot cache (`HOT_CACHE_ENABLED=true`). Hot cache is dormant by default. Enable it when real-time dashboarding is needed.

### GET /grafana/search, POST /grafana/query

Grafana SimpleJSON datasource.

### GET /export/powerbi, /export/csv, /export/json

Export endpoints for BI tools.

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
