# LogicMonitor Data Pipeline - API Documentation

**Version:** 13.1
**Base URL:** `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io`
**Protocol:** HTTPS

---

## Table of Contents

1. [Authentication](#authentication)
2. [Data Ingestion](#data-ingestion)
3. [Health & Monitoring](#health--monitoring)
4. [Data Export](#data-export)
5. [Error Codes](#error-codes)

---

## Authentication

### API Key Authentication

**Status:** NOT IMPLEMENTED

API key authentication is not currently implemented. Endpoints are publicly accessible.

### Azure AD Authentication (Database)

PostgreSQL connections use Azure AD managed identity tokens that are refreshed automatically every 45 minutes.

---

## Data Ingestion

### POST /api/HttpIngest

Ingest OTLP (OpenTelemetry Protocol) formatted metrics data.

**Headers:**
```http
Content-Type: application/json
Content-Encoding: gzip (optional)
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
  "id": 12345,
  "timestamp": "2025-01-14T12:00:00Z"
}
```

**Gzip Compression:**
```bash
curl -X POST https://{host}/api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @payload.json.gz
```

---

## Health & Monitoring

### GET /api/health

Health check endpoint with component-level status.

**Response:** `200 OK` (healthy) or `503 Service Unavailable` (degraded)
```json
{
  "status": "healthy",
  "timestamp": "2025-01-14T12:00:00Z",
  "version": "13.1-no-streaming",
  "components": {
    "database": "healthy",
    "background_tasks": "3/3 running"
  }
}
```

**Components:**
- `database`: PostgreSQL connection pool health
- `background_tasks`: Number of running background tasks (data processing, health monitoring, token refresh)

### GET /api/metrics/summary

**Status:** NOT IMPLEMENTED

This endpoint does not exist in the current implementation.

---

## Data Export

### GET /metrics

Export metrics in Prometheus text format.

**Query Parameters:**
None. Returns all metrics from the last hour (default configuration in code).

**Example:**
```bash
curl "https://{host}/metrics"
```

**Response:** `200 OK` (text/plain)
```
# HELP cpu_usage CPU usage percentage
# TYPE cpu_usage gauge
cpu_usage{service="my-service",host="host-01"} 45.5 1673000000000

# HELP memory_bytes Memory usage in bytes
# TYPE memory_bytes gauge
memory_bytes{service="my-service",host="host-01"} 8589934592 1673000000000
```

### Grafana SimpleJSON Datasource

#### GET /grafana/search

Search for available metrics.

**Request:**
```json
{
  "target": "cpu"
}
```

**Response:** `200 OK`
```json
[
  "cpu.usage",
  "cpu.idle",
  "cpu.system"
]
```

#### POST /grafana/query

Query time-series data.

**Request:**
```json
{
  "targets": [
    {"target": "cpu.usage"}
  ],
  "range": {
    "from": "2025-01-14T00:00:00Z",
    "to": "2025-01-14T12:00:00Z"
  },
  "maxDataPoints": 1000
}
```

**Response:** `200 OK`
```json
[
  {
    "target": "cpu.usage",
    "datapoints": [
      [45.5, 1673000000000],
      [46.2, 1673000060000]
    ]
  }
]
```

### GET /export/powerbi

PowerBI-compatible JSON export.

**Query Parameters:**
- `start_time` (optional): ISO 8601 timestamp (default: 24 hours ago)
- `end_time` (optional): ISO 8601 timestamp (default: now)

**Example:**
```bash
curl "https://{host}/export/powerbi?start_time=2025-01-14T00:00:00&end_time=2025-01-14T12:00:00"
```

**Response:** `200 OK`
```json
{
  "data": [
    {
      "metric_name": "cpu.usage",
      "resource_service": "my-service",
      "resource_host": "host-01",
      "value": 45.5,
      "timestamp": "2025-01-14T12:00:00Z"
    }
  ]
}
```

### GET /export/csv

Export metrics as CSV.

**Query Parameters:**
- `start_time` (optional): ISO 8601 timestamp (default: 24 hours ago)
- `end_time` (optional): ISO 8601 timestamp (default: now)

**Example:**
```bash
curl "https://{host}/export/csv?start_time=2025-01-14T00:00:00&end_time=2025-01-14T12:00:00" > metrics.csv
```

**Response:** `200 OK` (text/csv)
```csv
metric_name,resource_service,resource_host,value,timestamp
cpu.usage,my-service,host-01,45.5,2025-01-14T12:00:00Z
cpu.usage,my-service,host-01,46.2,2025-01-14T12:01:00Z
```

### GET /export/json

Export metrics as JSON.

**Query Parameters:**
- `start_time` (optional): ISO 8601 timestamp (default: 24 hours ago)
- `end_time` (optional): ISO 8601 timestamp (default: now)

**Example:**
```bash
curl "https://{host}/export/json?start_time=2025-01-14T00:00:00&end_time=2025-01-14T12:00:00"
```

**Response:** `200 OK` (application/json)
```json
{
  "metrics": [
    {
      "name": "cpu.usage",
      "resource": {
        "service": "my-service",
        "host": "host-01"
      },
      "datapoints": [
        {"value": 45.5, "timestamp": "2025-01-14T12:00:00Z"}
      ]
    }
  ]
}
```

---

## Error Codes

| Code | Description | Resolution |
|------|-------------|------------|
| 400 | Bad Request | Check request format and parameters |
| 404 | Not Found | Verify endpoint URL |
| 500 | Internal Server Error | Check logs, contact support |
| 503 | Service Unavailable | Database unavailable or service degraded |

**Error Response Format:**
```json
{
  "error": "Error description"
}
```

**Note:** Authentication (401/403) and rate limiting (429) are not currently implemented.

---

## Best Practices

### Data Ingestion

1. **Use gzip compression** for payloads > 1KB (set `Content-Encoding: gzip` header)
2. **Batch metrics** (recommended: 100-1000 metrics per request)
3. **Include timestamps** in Unix nanoseconds
4. **Set proper resource attributes** for better filtering

### Export

1. **Use Prometheus export** (`/metrics`) for time-series monitoring tools
2. **Use Grafana datasource** (`/grafana/search` and `/grafana/query`) for dashboards
3. **Use PowerBI export** (`/export/powerbi`) for business intelligence
4. **Use CSV/JSON** (`/export/csv`, `/export/json`) for ad-hoc analysis

### Performance

1. **Limit time ranges** when exporting data (default: last 24 hours)
2. **Use appropriate query parameters** (start_time, end_time) to reduce data volume
3. **Monitor application health** via `/api/health` endpoint

---

## Support

**Documentation:** https://github.com/logicmonitor/HttpIngest/blob/main/FEATURES.md
**Issues:** https://github.com/logicmonitor/HttpIngest/issues
**Health Status:** https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
