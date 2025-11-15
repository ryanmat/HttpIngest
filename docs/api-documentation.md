# LogicMonitor Data Pipeline - API Documentation

**Version:** 12.0.0
**Base URL:** `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io`
**Protocol:** HTTPS

---

## Table of Contents

1. [Authentication](#authentication)
2. [Data Ingestion](#data-ingestion)
3. [Health & Monitoring](#health--monitoring)
4. [Data Export](#data-export)
5. [Real-time Streaming](#real-time-streaming)
6. [Error Codes](#error-codes)
7. [Rate Limits](#rate-limits)

---

## Authentication

### API Key Authentication

Include API key in request headers:

```http
X-API-Key: your-api-key-here
```

### Azure AD Authentication (Database)

PostgreSQL connections use Azure AD tokens that expire every 90 minutes.

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

**Response:** `202 Accepted`
```json
{
  "id": 12345,
  "status": "accepted"
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

Basic health check for Azure Functions runtime.

**Response:** `200 OK`
```json
{
  "status": "healthy",
  "timestamp": "2025-01-14T12:00:00Z",
  "components": {
    "database": "healthy",
    "streaming": "healthy",
    "background_tasks": "3/3 running"
  }
}
```

### GET /api/health (FastAPI)

Detailed health check with component-level status.

**Response:** `200 OK`
```json
{
  "status": "healthy",
  "timestamp": "2025-01-14T12:00:00Z",
  "version": "12.0.0",
  "components": {
    "database": {
      "status": "healthy",
      "metric_count": 125000
    },
    "streaming": {
      "status": "healthy",
      "active_websockets": 15
    },
    "background_tasks": {
      "data_processor": "running",
      "metric_publisher": "running",
      "health_monitor": "running"
    }
  }
}
```

### GET /api/metrics/summary

Get metrics summary statistics.

**Response:** `200 OK`
```json
{
  "metrics": 42,
  "resources": 15,
  "total_datapoints": 125000,
  "oldest_data": "2025-01-01T00:00:00Z",
  "newest_data": "2025-01-14T12:00:00Z"
}
```

---

## Data Export

### GET /metrics/prometheus

Export metrics in Prometheus text format.

**Query Parameters:**
- `metrics` (optional): Comma-separated metric names
- `hours` (optional, default=1): Hours of data to export

**Example:**
```bash
curl "https://{host}/metrics/prometheus?metrics=cpu.usage,memory.bytes&hours=1"
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

#### GET /grafana

Health check for Grafana datasource.

**Response:** `200 OK`
```json
{
  "status": "ok",
  "message": "LogicMonitor Data Pipeline"
}
```

#### POST /grafana/search

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

### GET /api/odata/metrics

PowerBI OData export.

**Query Parameters:**
- `$skip` (optional, default=0): Number of records to skip
- `$top` (optional, default=1000, max=10000): Number of records to return

**Example:**
```bash
curl "https://{host}/api/odata/metrics?$skip=0&$top=1000"
```

**Response:** `200 OK`
```json
{
  "@odata.context": "https://{host}/api/odata/$metadata#metrics",
  "@odata.count": 125000,
  "value": [
    {
      "metric_name": "cpu.usage",
      "resource_service": "my-service",
      "resource_host": "host-01",
      "value": 45.5,
      "timestamp": "2025-01-14T12:00:00Z"
    }
  ],
  "@odata.nextLink": "https://{host}/api/odata/metrics?$skip=1000&$top=1000"
}
```

### GET /export/csv

Export metrics as CSV.

**Query Parameters:**
- `metrics` (required): Comma-separated metric names
- `hours` (optional, default=24): Hours of data

**Example:**
```bash
curl "https://{host}/export/csv?metrics=cpu.usage&hours=24" > metrics.csv
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
- `metrics` (required): Comma-separated metric names
- `hours` (optional, default=24): Hours of data
- `pretty` (optional, default=false): Pretty-print JSON

**Example:**
```bash
curl "https://{host}/export/json?metrics=cpu.usage&hours=1&pretty=true"
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

## Real-time Streaming

### WebSocket /ws

Real-time metric updates via WebSocket.

**Connection:**
```javascript
const ws = new WebSocket('wss://{host}/ws?client_id=my-client');

ws.onopen = () => {
  console.log('Connected');

  // Subscribe to metrics
  ws.send(JSON.stringify({
    action: 'subscribe',
    patterns: ['cpu.*', 'memory.*']
  }));
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  console.log('Update:', message);
};
```

**Message Format:**
```json
{
  "type": "metric_update",
  "metric_name": "cpu.usage",
  "resource": {
    "service": "my-service",
    "host": "host-01"
  },
  "value": 45.5,
  "timestamp": "2025-01-14T12:00:00Z",
  "sequence": 12345
}
```

**Subscription Actions:**
```json
{
  "action": "subscribe",
  "patterns": ["cpu.*", "memory.*"]
}

{
  "action": "unsubscribe",
  "patterns": ["cpu.*"]
}
```

**Rate Limiting:**
- Default: 10 messages/second per client
- Burst: 20 messages
- Backpressure notification on rate limit exceeded

### GET /sse

Server-Sent Events for one-way streaming.

**Connection:**
```javascript
const eventSource = new EventSource('https://{host}/sse?client_id=my-client');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Update:', data);
};

// Automatic reconnection with Last-Event-ID
```

**Event Format:**
```
id: 12345
event: metric_update
data: {"metric_name":"cpu.usage","value":45.5,"timestamp":"2025-01-14T12:00:00Z"}

```

---

## Error Codes

| Code | Description | Resolution |
|------|-------------|------------|
| 400 | Bad Request | Check request format and parameters |
| 401 | Unauthorized | Provide valid API key |
| 403 | Forbidden | Check API key permissions |
| 404 | Not Found | Verify endpoint URL |
| 429 | Too Many Requests | Reduce request rate |
| 500 | Internal Server Error | Check logs, contact support |
| 503 | Service Unavailable | Service temporarily down, retry later |

**Error Response Format:**
```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "Invalid OTLP payload format",
    "details": "Missing resourceMetrics field"
  }
}
```

---

## Rate Limits

### HTTP Endpoints

| Endpoint | Rate Limit | Burst |
|----------|------------|-------|
| /api/HttpIngest | 1000/min | 100 |
| /metrics/prometheus | 60/min | 10 |
| /export/* | 30/min | 5 |
| /api/health | Unlimited | - |

### WebSocket Connections

- **Max Connections:** 1000 concurrent
- **Messages/sec per client:** 10 (configurable)
- **Burst size:** 20 messages
- **Message buffer:** 1000 messages per client
- **Reconnection:** State preserved for 24 hours

### Rate Limit Headers

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 950
X-RateLimit-Reset: 1673000060
```

---

## Best Practices

### Data Ingestion

1. **Use gzip compression** for payloads > 1KB
2. **Batch metrics** (recommended: 100-1000 metrics per request)
3. **Include timestamps** in Unix nanoseconds
4. **Set proper resource attributes** for better filtering

### Export

1. **Use Prometheus export** for time-series monitoring
2. **Use Grafana datasource** for dashboards
3. **Use PowerBI** for business intelligence
4. **Use CSV/JSON** for ad-hoc analysis

### Real-time Streaming

1. **Specify client_id** for reconnection support
2. **Subscribe to specific patterns** to reduce traffic
3. **Handle backpressure** notifications
4. **Implement exponential backoff** on reconnection

### Performance

1. **Cache Prometheus/Grafana queries** (1-5 minutes)
2. **Use pagination** for large exports ($skip/$top)
3. **Limit time ranges** (default: last hour)
4. **Monitor rate limits** via headers

---

## Support

**Documentation:** https://github.com/logicmonitor/HttpIngest/blob/main/FEATURES.md
**Issues:** https://github.com/logicmonitor/HttpIngest/issues
**Health Status:** https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
