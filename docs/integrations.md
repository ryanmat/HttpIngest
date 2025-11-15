# Data Exporter Integrations

Complete guide for integrating LogicMonitor data pipeline with external systems.

## Table of Contents

1. [Prometheus Integration](#prometheus-integration)
2. [Grafana SimpleJSON Integration](#grafana-simplejson-integration)
3. [PowerBI Integration](#powerbi-integration)
4. [CSV/JSON Export](#csvjson-export)
5. [Webhook Notifications](#webhook-notifications)

---

## Prometheus Integration

### Overview

Export metrics in Prometheus text format for scraping by Prometheus servers.

### Usage Example

```python
from src.exporters import PrometheusExporter, TimeSeriesQuery
from datetime import datetime, timedelta

# Initialize exporter
exporter = PrometheusExporter(
    db_connection_string="postgresql://user:pass@host:5432/db"
)

# Create query for last hour of CPU metrics
query = TimeSeriesQuery(
    metric_names=["cpu.usage", "cpu.idle"],
    start_time=datetime.now() - timedelta(hours=1),
    end_time=datetime.now(),
    limit=1000
)

# Export metrics
prometheus_output = exporter.export_metrics(query, include_help=True)
print(prometheus_output)
```

### Expected Output

```
# HELP cpu_usage CPU_Usage (percent)
# TYPE cpu_usage gauge
cpu_usage{datasource="CPU_Usage",host_name="server-01",service_name="web-server"} 75.5 1700000000000
cpu_usage{datasource="CPU_Usage",host_name="server-01",service_name="web-server"} 80.2 1700000060000
# HELP cpu_idle CPU_Idle (percent)
# TYPE cpu_idle gauge
cpu_idle{datasource="CPU_Usage",host_name="server-01",service_name="web-server"} 24.5 1700000000000
```

### Prometheus Configuration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'logicmonitor'
    scrape_interval: 60s
    static_configs:
      - targets: ['your-api-host:8080']
    metrics_path: '/api/metrics/prometheus'
```

### FastAPI Endpoint Example

```python
from fastapi import FastAPI, Query
from src.exporters import PrometheusExporter, TimeSeriesQuery

app = FastAPI()
exporter = PrometheusExporter("postgresql://...")

@app.get("/api/metrics/prometheus")
async def prometheus_metrics(
    metrics: str = Query(None, description="Comma-separated metric names"),
    hours: int = Query(1, description="Hours of data to export")
):
    metric_list = metrics.split(",") if metrics else None

    query = TimeSeriesQuery(
        metric_names=metric_list,
        start_time=datetime.now() - timedelta(hours=hours),
        end_time=datetime.now()
    )

    return Response(
        content=exporter.export_metrics(query),
        media_type="text/plain; version=0.0.4"
    )
```

---

## Grafana SimpleJSON Integration

### Overview

Implement Grafana SimpleJSON datasource API for dynamic dashboards.

### Required Endpoints

The SimpleJSON datasource requires these endpoints:

1. **Health Check**: `GET /`
2. **Search**: `POST /search`
3. **Query**: `POST /query`
4. **Annotations**: `POST /annotations` (optional)

### FastAPI Implementation

```python
from fastapi import FastAPI, Body
from src.exporters import GrafanaSimpleJSONDataSource
from typing import Dict, Any, List

app = FastAPI()
datasource = GrafanaSimpleJSONDataSource("postgresql://...")

@app.get("/")
async def health():
    """Health check endpoint."""
    return datasource.health_check()

@app.post("/search")
async def search(request: Dict[str, Any] = Body(...)):
    """Return available metrics."""
    target = request.get("target", "")
    return datasource.search(target if target else None)

@app.post("/query")
async def query(request: Dict[str, Any] = Body(...)):
    """Return time-series data."""
    return datasource.query(request)

@app.post("/annotations")
async def annotations(request: Dict[str, Any] = Body(...)):
    """Return annotations (alerts)."""
    return datasource.annotations(request)
```

### Grafana Configuration

1. **Add Data Source**:
   - Type: SimpleJSON
   - URL: `http://your-api-host:8080`
   - Access: Server (default)

2. **Test Connection**: Click "Save & Test"

3. **Create Dashboard**:
   - Add panel
   - Select your SimpleJSON datasource
   - In query editor, type metric name (e.g., "cpu.usage")
   - Grafana will autocomplete available metrics

### Query Example

Grafana sends this request to `/query`:

```json
{
  "targets": [
    {"target": "cpu.usage", "refId": "A"}
  ],
  "range": {
    "from": "2023-01-01T00:00:00Z",
    "to": "2023-01-01T01:00:00Z"
  },
  "interval": "1m"
}
```

Response format:

```json
[
  {
    "target": "cpu.usage",
    "datapoints": [
      [75.5, 1672531200000],
      [76.0, 1672531260000],
      [77.2, 1672531320000]
    ]
  }
]
```

### Variable Queries

Use Grafana variables for dynamic dashboards:

```
Query: *
Regex: /cpu.*/
```

This creates a dropdown with all CPU-related metrics.

---

## PowerBI Integration

### Overview

Export data in OData-compatible format for PowerBI Desktop and PowerBI Service.

### FastAPI Endpoint

```python
from fastapi import FastAPI, Query
from src.exporters import PowerBIExporter, TimeSeriesQuery

app = FastAPI()
exporter = PowerBIExporter("postgresql://...")

@app.get("/api/odata/metrics")
async def powerbi_data(
    metric: str = Query(None, description="Metric name filter"),
    skip: int = Query(0, description="Skip N records (pagination)"),
    top: int = Query(1000, description="Return top N records")
):
    """PowerBI OData endpoint."""
    query = TimeSeriesQuery(
        metric_names=[metric] if metric else None,
        start_time=datetime.now() - timedelta(days=7)
    )

    return exporter.export_data(query, skip=skip, top=top)
```

### PowerBI Desktop Setup

1. **Get Data**:
   - Click "Get Data" → "Web"
   - URL: `http://your-api-host:8080/api/odata/metrics`
   - Click "OK"

2. **Authentication**:
   - Choose "Anonymous" (or configure OAuth if needed)

3. **Load Data**:
   - PowerBI will detect the OData feed
   - Select "value" table
   - Click "Load"

### PowerBI Query Editor

The data will load with these columns:
- `metric`: Metric name
- `timestamp`: ISO timestamp
- `value`: Metric value
- `unit`: Unit of measurement
- `datasource`: Data source name
- `resource_*`: Flattened resource attributes
- `attr_*`: Flattened metric attributes

### PowerBI DAX Example

Create calculated columns:

```dax
// Convert timestamp to datetime
DateTime = DATEVALUE([timestamp]) + TIMEVALUE([timestamp])

// Calculate hourly average
HourlyAvg = AVERAGEX(
    FILTER(
        metrics,
        HOUR(metrics[DateTime]) = HOUR([DateTime])
    ),
    [value]
)
```

### PowerBI Service (Cloud)

1. **Publish Dataset**:
   - Publish from PowerBI Desktop to PowerBI Service

2. **Configure Refresh**:
   - Settings → Data source credentials
   - Configure scheduled refresh (up to 8x daily)

3. **Gateway** (if using on-prem data):
   - Install PowerBI Gateway
   - Configure data source connection

---

## CSV/JSON Export

### Overview

General purpose export for analysis tools, Excel, Python scripts, etc.

### CSV Export Example

```python
from src.exporters import CSVJSONExporter, TimeSeriesQuery

exporter = CSVJSONExporter("postgresql://...")

# Export last 24 hours as CSV
query = TimeSeriesQuery(
    metric_names=["cpu.usage", "memory.usage"],
    start_time=datetime.now() - timedelta(days=1),
    limit=10000
)

csv_data = exporter.export_csv(query, flatten_json=True)

# Save to file
with open("metrics_export.csv", "w") as f:
    f.write(csv_data)
```

### JSON Export Example

```python
json_data = exporter.export_json(query, pretty=True)

with open("metrics_export.json", "w") as f:
    f.write(json_data)
```

### FastAPI Endpoints

```python
from fastapi import FastAPI
from fastapi.responses import Response, StreamingResponse
import io

app = FastAPI()
exporter = CSVJSONExporter("postgresql://...")

@app.get("/api/export/csv")
async def export_csv(
    metrics: str = Query(..., description="Comma-separated metrics"),
    hours: int = Query(24)
):
    """Export metrics as CSV."""
    query = TimeSeriesQuery(
        metric_names=metrics.split(","),
        start_time=datetime.now() - timedelta(hours=hours)
    )

    csv_data = exporter.export_csv(query)

    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=metrics.csv"}
    )

@app.get("/api/export/json")
async def export_json(
    metrics: str = Query(...),
    hours: int = Query(24),
    pretty: bool = Query(False)
):
    """Export metrics as JSON."""
    query = TimeSeriesQuery(
        metric_names=metrics.split(","),
        start_time=datetime.now() - timedelta(hours=hours)
    )

    json_data = exporter.export_json(query, pretty=pretty)

    return Response(content=json_data, media_type="application/json")
```

### Excel Import

1. **Open Excel** → Data → Get Data → From Text/CSV
2. Select exported CSV file
3. Excel will auto-detect columns
4. Click "Load"

### Python Analysis Example

```python
import pandas as pd
import json

# Load JSON export
with open("metrics_export.json") as f:
    data = json.load(f)

df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# Calculate statistics
print(df.groupby('metric_name')['value'].describe())

# Plot
df[df['metric_name'] == 'cpu.usage']['value'].plot()
```

---

## Webhook Notifications

### Overview

Send real-time alerts to external systems (Slack, PagerDuty, custom apps).

### Basic Usage

```python
from src.exporters import WebhookNotifier, WebhookConfig, AlertEvent
from datetime import datetime

notifier = WebhookNotifier()

# Configure webhook
config = WebhookConfig(
    url="https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
    method="POST",
    headers={"Content-Type": "application/json"},
    timeout=10,
    retry_count=3
)

# Create alert
alert = AlertEvent(
    alert_id="alert-12345",
    severity="critical",
    metric_name="cpu.usage",
    resource={"host": "server-01", "service": "web-app"},
    current_value=95.0,
    threshold=90.0,
    message="CPU usage critically high on server-01",
    timestamp=datetime.now(),
    anomaly_score=0.98,
    metadata={"detector": "LSTM", "confidence": 0.95}
)

# Send alert
result = notifier.send_alert(alert, config)

if result["success"]:
    print(f"Alert sent successfully (attempt {result['attempt']})")
else:
    print(f"Alert failed: {result['error']}")
```

### HMAC Authentication

For secure webhooks with signature verification:

```python
config = WebhookConfig(
    url="https://your-app.com/webhook",
    method="POST",
    secret="your-secret-key",  # HMAC signing key
    timeout=10,
    retry_count=3
)

result = notifier.send_alert(alert, config)
```

The webhook will include header:
```
X-Webhook-Signature: sha256=abc123...
```

Verify signature in your webhook endpoint:

```python
import hmac
import hashlib

def verify_signature(payload: str, signature: str, secret: str) -> bool:
    expected = hmac.new(
        secret.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()

    return signature == f"sha256={expected}"
```

### Slack Integration

```python
# Slack webhook config
slack_config = WebhookConfig(
    url="https://hooks.slack.com/services/T00/B00/XXX",
    method="POST"
)

# Format alert for Slack
alert = AlertEvent(
    alert_id="alert-001",
    severity="warning",
    metric_name="disk.usage",
    resource={"host": "db-server"},
    current_value=85.0,
    threshold=80.0,
    message=":warning: Disk usage high on db-server",
    timestamp=datetime.now()
)

notifier.send_alert(alert, slack_config)
```

### PagerDuty Integration

```python
pagerduty_config = WebhookConfig(
    url="https://events.pagerduty.com/v2/enqueue",
    method="POST",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Token YOUR_INTEGRATION_KEY"
    }
)

alert = AlertEvent(
    alert_id="pager-001",
    severity="critical",
    metric_name="service.availability",
    resource={"service": "api-gateway"},
    current_value=0.0,
    threshold=1.0,
    message="API Gateway down - immediate action required",
    timestamp=datetime.now()
)

notifier.send_alert(alert, pagerduty_config)
```

### Custom Webhook Endpoint Example

```python
from fastapi import FastAPI, Header, Request, HTTPException

app = FastAPI()

@app.post("/webhook/alerts")
async def receive_alert(
    request: Request,
    x_webhook_signature: str = Header(None)
):
    """Receive and verify webhook alerts."""
    body = await request.body()

    # Verify HMAC signature
    if x_webhook_signature:
        if not verify_signature(body.decode(), x_webhook_signature, "secret"):
            raise HTTPException(status_code=401, detail="Invalid signature")

    # Parse alert
    alert = await request.json()

    # Process alert
    print(f"Received alert: {alert['alert_id']}")
    print(f"Severity: {alert['severity']}")
    print(f"Metric: {alert['metric_name']} = {alert['current_value']}")

    # Take action based on severity
    if alert['severity'] == 'critical':
        # Send page to on-call engineer
        send_page(alert)
    elif alert['severity'] == 'warning':
        # Create ticket
        create_ticket(alert)

    return {"status": "received", "alert_id": alert['alert_id']}
```

---

## Integration Best Practices

### 1. Authentication

Always use authentication for production:
- API Keys in headers
- OAuth 2.0 for delegated access
- HMAC signatures for webhooks

### 2. Rate Limiting

Implement rate limiting to prevent abuse:

```python
from fastapi import FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter

@app.get("/api/metrics/prometheus")
@limiter.limit("60/minute")  # 60 requests per minute
async def prometheus_metrics():
    ...
```

### 3. Caching

Cache expensive queries:

```python
from functools import lru_cache
from datetime import datetime

@lru_cache(maxsize=100)
def get_metrics_cached(metric_name: str, hour: int):
    """Cache metrics by hour."""
    query = TimeSeriesQuery(
        metric_names=[metric_name],
        start_time=datetime.fromtimestamp(hour * 3600),
        end_time=datetime.fromtimestamp((hour + 1) * 3600)
    )
    return exporter.export_json(query)
```

### 4. Pagination

Always paginate large datasets:

```python
@app.get("/api/metrics")
async def get_metrics(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000)
):
    # Enforce maximum page size
    limit = min(limit, 1000)

    query = TimeSeriesQuery(limit=limit)
    # Use skip/limit for pagination
    ...
```

### 5. Error Handling

Graceful error handling:

```python
from fastapi import HTTPException

@app.get("/api/export/csv")
async def export_csv(metrics: str):
    try:
        query = TimeSeriesQuery(metric_names=metrics.split(","))
        return exporter.export_csv(query)
    except psycopg2.Error as e:
        raise HTTPException(status_code=503, detail="Database unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

### 6. Monitoring

Monitor your exporters:

```python
from prometheus_client import Counter, Histogram

export_requests = Counter('export_requests_total', 'Total export requests')
export_duration = Histogram('export_duration_seconds', 'Export duration')

@app.get("/api/export/json")
@export_duration.time()
async def export_json(metrics: str):
    export_requests.inc()
    ...
```

---

## Troubleshooting

### Grafana: "Cannot connect to datasource"

- Check URL is accessible from Grafana server
- Verify CORS headers if using browser access
- Check firewall rules

### PowerBI: "Unable to connect"

- Ensure OData endpoint returns valid JSON
- Check `@odata.count` field exists
- Verify pagination with `$skip` and `$top` works

### Prometheus: "No metrics found"

- Verify metrics endpoint returns text/plain
- Check metric names don't have invalid characters
- Ensure timestamps are in milliseconds

### Webhooks: "Connection timeout"

- Increase timeout value
- Check webhook URL is reachable
- Verify HMAC signature (if used)
- Check retry logic is working

---

## Performance Optimization

### Database Indexes

Ensure proper indexes exist:

```sql
CREATE INDEX idx_metric_data_timestamp ON metric_data(timestamp DESC);
CREATE INDEX idx_metric_data_composite ON metric_data(resource_id, metric_definition_id, timestamp DESC);
CREATE INDEX idx_metric_definitions_name ON metric_definitions(name);
```

### Connection Pooling

Use connection pooling:

```python
from psycopg2 import pool

connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn="postgresql://..."
)

class DatabaseConnection:
    def __enter__(self):
        self.conn = connection_pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        connection_pool.putconn(self.conn)
```

### Query Optimization

Limit query ranges:

```python
# Bad: Query entire history
query = TimeSeriesQuery(metric_names=["cpu.usage"])

# Good: Limit to specific time range
query = TimeSeriesQuery(
    metric_names=["cpu.usage"],
    start_time=datetime.now() - timedelta(days=1),
    end_time=datetime.now(),
    limit=1000
)
```
