# Description: API reference for the HttpIngest service.
# Description: Covers ingestion, health probes, and Prometheus metrics.

# HttpIngest API Documentation

**Version:** 1.0.0
**Protocol:** HTTPS
**Storage:** Azure Data Lake Gen2 (Parquet, Hive-partitioned)

---

## Table of Contents

1. [Authentication](#authentication)
2. [Data Ingestion](#data-ingestion)
3. [Health and Monitoring](#health-and-monitoring)
4. [Error Codes](#error-codes)

---

## Authentication

### `/api/HttpIngest` requires a bearer token

Every POST to `/api/HttpIngest` must carry an `Authorization: Bearer <token>`
header where `<token>` matches the server-side `INGEST_BEARER_TOKEN`
environment variable. The check uses a constant-time compare
(`secrets.compare_digest`) to avoid leaking the expected value via timing.

| Condition | Response |
|---|---|
| `Authorization: Bearer <token>` matches `INGEST_BEARER_TOKEN` | Request proceeds |
| Header missing or non-Bearer scheme | `401 Unauthorized` |
| Bearer scheme with wrong token | `401 Unauthorized` (`{"detail": "Invalid bearer token"}`) |
| Server has no `INGEST_BEARER_TOKEN` set | `503 Service Unavailable` (fail-closed) |

This is RFC 6750-compatible bearer auth — the same scheme most OTLP-source
clients support natively (e.g. an OTLP HTTP exporter or a Collector publisher
configured with bearer-token auth).

The `/health`, `/api/health`, and `/metrics` endpoints stay open so Container
Apps probes and Prometheus scraping work without credentials. They expose
component status and counters only — no secrets and no payload data.

For defense-in-depth, also restrict ingress at the Azure layer (Container
Apps IP restrictions, internal-VNet ingress, Front Door, API Management).
The application-layer token alone is not a complete security posture against
sustained brute-force from the open internet.

### Azure Authentication

The service authenticates to Azure Data Lake Gen2 via
`DefaultAzureCredential`. In production, set `USE_MANAGED_IDENTITY=true` and
grant the Container App's managed identity the **Storage Blob Data
Contributor** role on the target ADLS account.

---

## Data Ingestion

### `POST /api/HttpIngest`

Accept OTLP (OpenTelemetry Protocol) JSON metrics.

**Headers:**

```http
Content-Type: application/json
Authorization: Bearer <token>               # required (matches INGEST_BEARER_TOKEN)
Content-Encoding: gzip                      # optional, recommended for >1 KB
```

**Request Body** (truncated example):

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

**Response (200 OK):**

```json
{
  "status": "success",
  "stats": {
    "resources": 1,
    "scopes": 1,
    "metric_definitions": 1,
    "metric_data": 1,
    "datalake_written": 1,
    "errors": []
  },
  "timestamp": "2026-04-30T12:00:00Z"
}
```

**Example (gzip):**

```bash
curl -X POST "https://<your-app>/api/HttpIngest" \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  -H "Authorization: Bearer $INGEST_BEARER_TOKEN" \
  --data-binary @payload.json.gz
```

---

## Health and Monitoring

### `GET /health`

Lightweight root health probe used by Container Apps.

```json
{
  "status": "healthy",
  "timestamp": "2026-04-30T23:31:19Z",
  "version": "1.0.0"
}
```

Returns `503` if the Data Lake writer failed to initialize at startup.

### `GET /api/health`

Detailed health probe with per-component status.

```json
{
  "status": "healthy",
  "timestamp": "2026-04-30T23:31:19Z",
  "version": "1.0.0",
  "components": {
    "datalake": {
      "status": "healthy",
      "buffer": {
        "metric_data_buffered": 1024,
        "resources_buffered": 64,
        "scopes_buffered": 16,
        "metric_definitions_buffered": 256,
        "flush_threshold": 50000
      }
    },
    "ingestion_router": {
      "config": {"write_to_datalake": true},
      "datalake": { "...": "buffer stats" }
    },
    "background_tasks": {
      "running": 1,
      "total": 1,
      "tasks": ["datalake_flush"]
    }
  }
}
```

Returns `503` if any component reports `degraded` or `not initialized`.

### `GET /metrics`

Prometheus-format metrics from in-memory counters (no DB dependency).

```
# HELP httpingest_requests_total Total HTTP ingest requests received
# TYPE httpingest_requests_total counter
httpingest_requests_total 12345
httpingest_requests_success_total 12345
httpingest_requests_error_total 0
httpingest_metrics_ingested_total 250000
httpingest_datalake_flushes_total 50
httpingest_datalake_records_written_total 240000
httpingest_datalake_buffer_size 512
httpingest_info{version="1.0.0"} 1
```

---

## Error Codes

| Code | Description | Resolution |
|------|-------------|------------|
| 400  | Bad Request | Invalid JSON or missing `resourceMetrics` field |
| 401  | Unauthorized | Missing or wrong `Authorization: Bearer <token>` |
| 500  | Internal Server Error | Check container logs |
| 503  | Service Unavailable | Data Lake writer / ingestion router not initialized, OR `INGEST_BEARER_TOKEN` unset on the server |

**Error response shape:**

```json
{
  "error": "description"
}
```

---

## Best Practices

1. **Use gzip** for payloads >1 KB (`Content-Encoding: gzip`).
2. **Batch metrics** (100-1000 per request) instead of one-at-a-time.
3. **Include nanosecond timestamps** (`timeUnixNano`).
4. **Set meaningful resource attributes** (`service.name`, `host.name`,
   `environment`, etc.) so downstream consumers can filter efficiently.
