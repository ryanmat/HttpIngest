# LogicMonitor Data Pipeline - Feature Inventory

This document provides a comprehensive inventory of all features implemented in the LogicMonitor Data Pipeline and how to access them.

## Table of Contents

1. [Data Ingestion](#data-ingestion)
2. [Data Processing](#data-processing)
3. [Data Aggregation](#data-aggregation)
4. [ML Pipeline](#ml-pipeline)
5. [Data Export](#data-export)
6. [Real-time Streaming](#real-time-streaming)
7. [Background Tasks](#background-tasks)
8. [Health & Monitoring](#health--monitoring)
9. [Database](#database)

---

## Data Ingestion

### HTTP OTLP Ingestion
**Endpoint:** `POST /api/HttpIngest`
**Implementation:** `function_app.py:287`
**Features:**
- Accepts OTLP (OpenTelemetry Protocol) format data
- Supports gzip compression
- Azure AD token authentication for PostgreSQL
- Stores raw payloads in `lm_metrics` table

**Usage:**
```bash
curl -X POST https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d @otlp_payload.json
```

**Test Coverage:** `tests/test_e2e_integration.py::test_otlp_ingestion_to_database`

---

## Data Processing

### OTLP Parser
**Implementation:** `src/otlp_parser.py`
**Features:**
- Parses OTLP resourceMetrics structure
- Extracts resources, metrics, and data points
- Handles gauge, sum, histogram metric types
- Normalizes attribute types (string, int, double, bool)

**Accessed By:** Background data processing loop

**Test Coverage:** `tests/test_otlp_parser.py` (27 tests)

### Data Processor
**Implementation:** `src/data_processor.py`
**Features:**
- Normalizes OTLP data into relational schema
- Idempotent processing with status tracking
- Batch processing with error handling
- Resource deduplication with fingerprinting
- Metric definition management
- Data point insertion with type handling (int/double)

**Accessed By:** Background data processing loop (`function_app.py:162`)

**Database Tables:**
- `resources` - Deduplicated resources with attributes
- `datasources` - Instrumentation scope tracking
- `metric_definitions` - Metric metadata (name, description, unit, type)
- `metric_data` - Time-series data points
- `processing_status` - Processing state tracking

**Test Coverage:** `tests/test_data_processor.py` (25 tests)

---

## Data Aggregation

### Aggregator
**Implementation:** `src/aggregator.py`
**Features:**
- Hourly aggregates (min, max, avg, count, sum)
- Daily aggregates
- Efficient time-bucket based aggregation
- Metric-level granularity

**Accessed By:** Can be called from background tasks or scheduled jobs

**Database Tables:**
- `metric_aggregates_hourly`
- `metric_aggregates_daily`

**Test Coverage:** `tests/test_aggregator.py`

### Materialized Views
**Implementation:** Database migration `alembic/versions/20251114_1941-6f6f6a0c623f_add_materialized_views_for_query_.py`
**Features:**
- `mv_latest_metrics` - Latest value per metric
- `mv_metric_stats` - Statistical summaries
- Refreshable views for query performance

**Test Coverage:** `tests/test_materialized_views.py`

---

## ML Pipeline

### Feature Engineering
**Implementation:** `src/feature_engineering.py`
**Features:**
- Rolling statistics (mean, std, min, max)
- Lag features (1h, 6h, 24h lags)
- Rate of change features
- Time-based features (hour, day of week)
- Handles missing data

**Accessed By:** Can be integrated into background tasks

**Test Coverage:** `tests/test_feature_engineering.py`

### Anomaly Detection
**Implementation:** `src/anomaly_detector.py`
**Features:**
- Isolation Forest algorithm
- Statistical outlier detection (Z-score, IQR)
- Adaptive thresholds
- Anomaly scoring and ranking
- Historical baseline comparison

**Accessed By:** Can be integrated into background tasks

**Test Coverage:** `tests/test_anomaly_detector.py`

### Time Series Forecasting
**Implementation:** `src/predictor.py`
**Features:**
- Prophet forecasting (Facebook's time series library)
- ARIMA models
- Linear regression baseline
- Multi-horizon predictions
- Confidence intervals
- Trend and seasonality decomposition

**Accessed By:** Can be integrated into background tasks

**Test Coverage:** `tests/test_predictor.py`

---

## Data Export

### Prometheus Metrics Export
**Endpoint:** `GET /metrics/prometheus`
**Implementation:** `function_app.py:404` → `src/exporters.py:PrometheusExporter`
**Features:**
- Prometheus text format (v0.0.4)
- Metric name sanitization
- Label generation from resource attributes
- Help and type annotations
- Timestamp support

**Usage:**
```bash
# Get all metrics
curl https://{host}/metrics/prometheus

# Filter by metric names
curl "https://{host}/metrics/prometheus?metrics=cpu.usage,memory.bytes&hours=1"
```

**Integration:** Add to Prometheus `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'lm-pipeline'
    static_configs:
      - targets: ['ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io']
    metrics_path: '/metrics/prometheus'
```

**Test Coverage:** `tests/test_exporters.py::test_prometheus_export`

### Grafana SimpleJSON Datasource
**Endpoints:**
- `GET /grafana` - Health check
- `POST /grafana/search` - Metric search
- `POST /grafana/query` - Time-series query

**Implementation:** `src/exporters.py:GrafanaSimpleJSONDataSource`
**Features:**
- SimpleJSON datasource API compliance
- Metric search with pattern matching
- Time-series data transformation
- Annotation support

**Usage:**
1. Add datasource in Grafana: Type = "SimpleJSON"
2. URL: `https://{host}/grafana`
3. Query metrics in dashboards

**Test Coverage:** `tests/test_exporters.py::TestGrafanaSimpleJSON`

### PowerBI OData Export
**Endpoint:** `GET /api/odata/metrics`
**Implementation:** `function_app.py:441` → `src/exporters.py:PowerBIExporter`
**Features:**
- OData v4 format
- Pagination with `$skip` and `$top`
- Flattened metric data for PowerBI consumption
- Next link generation for large datasets

**Usage:**
```bash
# Get first 1000 metrics
curl "https://{host}/api/odata/metrics?$top=1000"

# Pagination
curl "https://{host}/api/odata/metrics?$skip=1000&$top=1000"
```

**PowerBI Integration:**
1. Get Data → Web
2. URL: `https://{host}/api/odata/metrics`
3. PowerBI auto-detects OData format

**Test Coverage:** `tests/test_exporters.py::TestPowerBIExporter`

### CSV Export
**Endpoint:** `GET /export/csv`
**Implementation:** `function_app.py:453` → `src/exporters.py:CSVJSONExporter`
**Features:**
- Comma-separated values
- Header row with column names
- Metric name, resource attributes, value, timestamp columns
- Resource attributes flattened

**Usage:**
```bash
curl "https://{host}/export/csv?metrics=cpu.usage,memory.bytes&hours=24" > metrics.csv
```

**Test Coverage:** `tests/test_exporters.py::test_csv_export`

### JSON Export
**Endpoint:** `GET /export/json`
**Implementation:** `function_app.py:472` → `src/exporters.py:CSVJSONExporter`
**Features:**
- Structured JSON format
- Pretty-print option
- Nested metric data

**Usage:**
```bash
# Compact JSON
curl "https://{host}/export/json?metrics=cpu.usage&hours=1"

# Pretty-printed JSON
curl "https://{host}/export/json?metrics=cpu.usage&hours=1&pretty=true"
```

**Test Coverage:** `tests/test_exporters.py::test_json_export`

### Webhook Notifications
**Implementation:** `src/exporters.py:WebhookNotifier`
**Features:**
- HTTP POST webhooks
- HMAC-SHA256 signatures for authentication
- Retry logic with exponential backoff
- Configurable timeout
- Alert event structure

**Usage:** Programmatic only (integrate with alerting logic)

**Test Coverage:** `tests/test_exporters.py::TestWebhookNotifier`

---

## Real-time Streaming

### WebSocket Streaming
**Endpoint:** `WebSocket /ws`
**Implementation:** `function_app.py:488` → `src/realtime.py:WebSocketManager`
**Features:**
- Bidirectional communication
- Client subscriptions to metric patterns
- Rate limiting per client (token bucket)
- Message buffering on backpressure
- Reconnection with message replay
- Client state persistence (24 hours)

**Usage:**
```javascript
const ws = new WebSocket('wss://{host}/ws?client_id=my-client');

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  console.log('Received:', message);
};

// Subscribe to metrics
ws.send(JSON.stringify({
  action: 'subscribe',
  patterns: ['cpu.*', 'memory.*']
}));
```

**Test Coverage:** `tests/test_realtime.py::TestWebSocketManager` (22 tests)

### Server-Sent Events (SSE)
**Endpoint:** `GET /sse`
**Implementation:** `function_app.py:496` → `src/realtime.py:SSEManager`
**Features:**
- One-way server-to-client streaming
- Event ID tracking
- Last-Event-ID resume support
- Automatic reconnection

**Usage:**
```javascript
const eventSource = new EventSource('https://{host}/sse?client_id=my-client');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Update:', data);
};
```

**Test Coverage:** `tests/test_realtime.py::TestSSEManager`

### Pub/Sub Messaging
**Implementation:** `src/realtime.py:MessageBroker`
**Features:**
- Redis-backed pub/sub (when available)
- In-memory fallback broker
- Channel-based routing
- Multiple subscribers per channel

**Configuration:**
```bash
# Use Redis
REDIS_URL=redis://your-redis-host:6379
USE_REDIS=true

# Use in-memory
USE_REDIS=false
```

**Test Coverage:** `tests/test_realtime.py::TestMessageBroker`

### Rate Limiting
**Implementation:** `src/realtime.py:RateLimiter`
**Features:**
- Token bucket algorithm
- Configurable rate and burst size
- Per-client rate limiting
- Backpressure signaling

**Configuration:**
```bash
RATE_LIMIT_MESSAGES_PER_SECOND=50
RATE_LIMIT_BURST_SIZE=100
```

**Test Coverage:** `tests/test_realtime.py::TestRateLimiter`

---

## Background Tasks

### Data Processing Loop
**Implementation:** `function_app.py:162`
**Interval:** 30 seconds (configurable via `DATA_PROCESSING_INTERVAL`)
**Function:**
- Polls `lm_metrics` table for unprocessed records
- Calls `DataProcessor` to normalize data
- Updates `processing_status` table

### Metric Publishing Loop
**Implementation:** `function_app.py:200`
**Interval:** 10 seconds (configurable via `METRIC_PUBLISHING_INTERVAL`)
**Function:**
- Queries recent metric data (last 1 minute)
- Publishes to `RealtimeStreamManager`
- Broadcasts to WebSocket/SSE clients

### Health Monitoring Loop
**Implementation:** `function_app.py:244`
**Interval:** 60 seconds (configurable via `HEALTH_MONITORING_INTERVAL`)
**Function:**
- Checks database connectivity
- Checks streaming service status
- Logs warnings for unhealthy components

### Graceful Shutdown
**Implementation:** `function_app.py:125`
**Features:**
- Shutdown event signaling
- Task cancellation
- Streaming manager cleanup
- Connection draining

---

## Health & Monitoring

### Health Check Endpoint
**Endpoints:**
- `GET /api/health` (Azure Functions) - Basic health
- `GET /api/health` (FastAPI) - Detailed health

**Implementation:** `function_app.py:350` and `function_app.py:508`
**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-14T12:00:00",
  "version": "1.0.0",
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

**Test Coverage:** `tests/test_e2e_integration.py::test_health_checks`

### Metrics Summary
**Endpoint:** `GET /api/metrics/summary`
**Implementation:** `function_app.py:554`
**Response:**
```json
{
  "metrics": 42,
  "resources": 15,
  "total_datapoints": 125000,
  "oldest_data": "2025-01-01T00:00:00",
  "newest_data": "2025-01-14T12:00:00"
}
```

---

## Database

### Alembic Migrations
**Location:** `alembic/versions/`
**Management:**
```bash
# Check current migration
uv run alembic current

# Upgrade to latest
uv run alembic upgrade head

# Create new migration
uv run alembic revision --autogenerate -m "description"

# Downgrade one version
uv run alembic downgrade -1
```

**Migrations:**
1. `20251114_1700-1657eb7b0db6_initialize_project_structure` - Initial schema
2. `20251114_1941-6f6f6a0c623f_add_materialized_views_for_query_` - Materialized views

**Test Coverage:** `tests/test_migrations.py`

### Schema
**Tables:**
- `lm_metrics` - Raw OTLP payloads
- `processing_status` - Processing state tracking
- `resources` - Deduplicated resources
- `datasources` - Instrumentation scopes
- `metric_definitions` - Metric metadata
- `metric_data` - Time-series data points
- `metric_aggregates_hourly` - Hourly aggregates
- `metric_aggregates_daily` - Daily aggregates

**Views:**
- `mv_latest_metrics` - Latest value per metric (materialized)
- `mv_metric_stats` - Statistical summaries (materialized)

**Test Coverage:** `tests/test_normalized_schema.py`

---

## Feature Access Summary

### All HTTP Endpoints

| Method | Path | Feature | Implementation |
|--------|------|---------|----------------|
| POST | `/api/HttpIngest` | OTLP Ingestion | function_app.py:287 |
| GET | `/api/health` | Health Check | function_app.py:350, 508 |
| GET | `/metrics/prometheus` | Prometheus Export | function_app.py:404 |
| GET | `/grafana` | Grafana Health | function_app.py:422 |
| POST | `/grafana/search` | Grafana Search | function_app.py:427 |
| POST | `/grafana/query` | Grafana Query | function_app.py:434 |
| GET | `/api/odata/metrics` | PowerBI Export | function_app.py:441 |
| GET | `/export/csv` | CSV Export | function_app.py:453 |
| GET | `/export/json` | JSON Export | function_app.py:472 |
| WebSocket | `/ws` | WebSocket Stream | function_app.py:488 |
| GET | `/sse` | SSE Stream | function_app.py:496 |
| GET | `/api/metrics/summary` | Metrics Summary | function_app.py:554 |

### Background Features (No Direct HTTP Access)

| Feature | Implementation | Accessed By |
|---------|----------------|-------------|
| OTLP Parser | src/otlp_parser.py | Data Processing Loop |
| Data Processor | src/data_processor.py | Data Processing Loop |
| Aggregator | src/aggregator.py | Can be scheduled |
| Feature Engineering | src/feature_engineering.py | Can be scheduled |
| Anomaly Detection | src/anomaly_detector.py | Can be scheduled |
| Forecasting | src/predictor.py | Can be scheduled |

### Database-Only Features

| Feature | Implementation | Access Method |
|---------|----------------|---------------|
| Materialized Views | Migration 6f6f6a0c623f | SQL queries |
| Hourly Aggregates | Table schema | SQL queries |
| Daily Aggregates | Table schema | SQL queries |

---

## Orphaned Code Analysis

**Status:** No orphaned code detected

All implemented features are either:
1. Exposed via HTTP endpoints
2. Used by background tasks
3. Available for programmatic integration
4. Implemented in database migrations

**Removed Files:**
- `src/function_app.py` (duplicate, removed)
- `src/function_app_original.py` (old version, removed)

---

## Next Steps for Feature Integration

1. **Query Endpoints**: Create `src/query_endpoints.py` to provide programmatic query API for metrics, resources, and aggregates
2. **Scheduled Jobs**: Add cron-like scheduling for aggregation and ML tasks
3. **API Authentication**: Add authentication layer for public endpoints
4. **GraphQL API**: Consider GraphQL for flexible metric querying
5. **Alerting**: Integrate anomaly detection with webhook notifications

---

## Testing Coverage

- **Unit Tests**: All components have dedicated test files
- **Integration Tests**: `tests/test_e2e_integration.py` covers end-to-end workflows
- **Infrastructure Tests**: `tests/test_infrastructure.py` validates fixtures and setup

**Run All Tests:**
```bash
uv run pytest tests/ -v
```

**Run E2E Tests Only:**
```bash
uv run pytest tests/test_e2e_integration.py -v
```

---

## Documentation

- **Docker Setup**: `docs/docker.md`
- **Azure Deployment**: `docs/azure.md`
- **Integrations**: `docs/integrations.md`
- **Migration Guide**: `MIGRATION_QUICK_START.md`
- **This Document**: `FEATURES.md`
