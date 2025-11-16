![](https://img.shields.io/badge/Code-Python-informational?style=flat&logo=python&color=ffe333&logoColor=ffffff)
![](https://img.shields.io/badge/Database-PostgreSQL-informational?style=flat&logo=postgresql&color=4169E1&logoColor=ffffff)
![](https://img.shields.io/badge/Cloud-Azure-informational?style=flat&logo=microsoftazure&color=0078D4&logoColor=ffffff)
![](https://img.shields.io/badge/Container-Docker-informational?style=flat&logo=docker&color=2496ED&logoColor=ffffff)

# LogicMonitor OTLP Data Pipeline

Async data pipeline for ingesting, normalizing, and exporting OTLP formatted JSON metrics from LogicMonitor Collector HTTPS Publisher.

## Architecture

**Components:**
- Azure Container Apps (async Python FastAPI application)
- Azure PostgreSQL Flexible Server (normalized schema with materialized views)
- Azure Managed Identity 
- Background processing

**Data Flow:**
```
LogicMonitor Collector → HTTPS Publisher → Container App (/api/HttpIngest)
    → PostgreSQL (normalized schema) → Materialized Views
    → Export APIs (Prometheus, Grafana, PowerBI, CSV/JSON)
    → Real-time Streaming (WebSocket/SSE)
```

**Key Features:**
- Async processing with asyncpg connection pooling
- Normalized database schema (resources, datasources, metrics, data points)
- Materialized views for aggregation (hourly, latest, resource summaries)
- Multiple export formats (Prometheus, Grafana SimpleJSON, PowerBI OData, CSV/JSON)
- Real-time streaming (WebSocket, Server-Sent Events)
- Managed identity authentication (no password storage)
- Auto-scaling (3-10 replicas based on load)
- Gzip compression support

## Prerequisites

- Azure subscription with Container Apps and PostgreSQL Flexible Server
- LogicMonitor account with Collector HTTPS Publisher enabled
- Azure CLI (`az`) version 2.50+
- Docker (for local development)
- Python 3.10+ with `uv` package manager

## Database Setup

### Schema Initialization

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Run migrations
uv run alembic upgrade head
```

### Database Tables

**Core Tables:**
- `lm_metrics` - Raw OTLP payloads
- `resources` - Resource metadata (service, host, instance)
- `datasources` - Data source definitions
- `metric_definitions` - Metric metadata (name, description, unit)
- `metric_data` - Normalized metric data points
- `processing_status` - Batch processing tracking

**Materialized Views:**
- `hourly_aggregates` - Hourly metric statistics (min, max, avg, count)
- `latest_metrics` - Most recent value per metric
- `resource_summary` - Resource-level aggregations
- `datasource_metrics` - Datasource-level summaries

**Regular Views:**
- `metrics_flat` - Denormalized view for reporting

## Environment Variables

### Required

```bash
# Database
POSTGRES_HOST=rm-postgres.postgres.database.azure.com
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=ryan.matuszewski@logicmonitor.com
USE_MANAGED_IDENTITY=true  # Use Azure AD authentication

# Application
ENVIRONMENT=production  # development, staging, production
```

### Optional

```bash
# Redis (optional, for distributed caching)
USE_REDIS=false
REDIS_URL=redis://localhost:6379

# Background Tasks
ENABLE_COLLECTOR_PUBLISHER=true
DATA_PROCESSING_INTERVAL=30  # seconds
HEALTH_MONITORING_INTERVAL=60  # seconds

# Streaming
MAX_WEBSOCKET_CONNECTIONS=100

# Application Insights
APPINSIGHTS_CONNECTION_STRING=InstrumentationKey=...

# Security
API_KEYS=key1,key2,key3  # Comma-separated
REQUIRE_HTTPS=true
CORS_ORIGINS=*  # Or specific origins
```

## Deployment

### Azure Container App

```bash
# Set variables
RESOURCE_GROUP="CTA_Resource_Group"
CONTAINER_APP="ca-cta-lm-ingest"
ACR_NAME="acrctalmhttps001"
IMAGE_NAME="lm-http-ingest"
VERSION="v12.4"

# Build and push image to ACR
az acr build \
  --registry $ACR_NAME \
  --resource-group $RESOURCE_GROUP \
  --image $IMAGE_NAME:$VERSION \
  --file Dockerfile.containerapp \
  .

# Update container app
az containerapp update \
  --name $CONTAINER_APP \
  --resource-group $RESOURCE_GROUP \
  --image $ACR_NAME.azurecr.io/$IMAGE_NAME:$VERSION \
  --set-env-vars \
    "USE_MANAGED_IDENTITY=true" \
    "ENABLE_COLLECTOR_PUBLISHER=true" \
  --revision-suffix $VERSION
```

### Managed Identity Setup

```bash
# Assign managed identity to container app
az containerapp identity assign \
  --name $CONTAINER_APP \
  --resource-group $RESOURCE_GROUP \
  --system-assigned

# Get managed identity principal ID
PRINCIPAL_ID=$(az containerapp identity show \
  --name $CONTAINER_APP \
  --resource-group $RESOURCE_GROUP \
  --query principalId -o tsv)

# Create Azure AD admin for PostgreSQL
az postgres flexible-server ad-admin create \
  --resource-group $RESOURCE_GROUP \
  --server-name rm-postgres \
  --display-name $CONTAINER_APP \
  --object-id $PRINCIPAL_ID
```

## API Endpoints

### Ingestion

```bash
# Ingest OTLP metrics (uncompressed)
curl -X POST https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d @otlp_payload.json

# Ingest OTLP metrics (gzip compressed)
curl -X POST https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @otlp_payload.json.gz
```

### Query Endpoints

```bash
# Health check
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health

# List all metrics
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/metrics

# Metrics summary
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/metrics/summary
```

### Export Endpoints

```bash
# Prometheus format (last 24 hours)
curl "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/metrics/prometheus?hours=24"

# CSV export (specific metric)
curl "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/export/csv?metrics=cpu.usage&hours=1" -o metrics.csv

# JSON export
curl "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/export/json?metrics=cpu.usage,memory.usage&hours=24" -o metrics.json
```

### Grafana SimpleJSON Datasource

```bash
# Health check
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/grafana/

# Search metrics
curl -X POST https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/grafana/search \
  -H "Content-Type: application/json" \
  -d '{"target": "cpu"}'

# Query time series
curl -X POST https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/grafana/query \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [{"target": "cpu.usage"}],
    "range": {
      "from": "2025-01-01T00:00:00Z",
      "to": "2025-01-01T23:59:59Z"
    },
    "maxDataPoints": 1000
  }'
```

### PowerBI OData

```bash
# Get OData metadata
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/powerbi/\$metadata

# Query metrics (OData format)
curl "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/powerbi/metrics?\$filter=metric_name eq 'cpu.usage'&\$top=1000"
```

## LogicMonitor Configuration

Configure Collector HTTPS Publisher in LogicMonitor:

```properties
# In Collector agent.conf or via UI
publisher.http.enable=true
publisher.http.url=https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest
publisher.http.format=otlp
publisher.http.compression=gzip
publisher.http.batch.size=100
publisher.http.batch.interval=30
```

Restart collector after configuration changes.

## Integration

### Grafana Setup

1. Install SimpleJSON datasource plugin
2. Add datasource with URL: `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/grafana`
3. Create dashboard and select metrics from datasource

### PowerBI Connection

**Option 1: Direct PostgreSQL Connection**
1. Get Data → PostgreSQL Database
2. Server: `rm-postgres.postgres.database.azure.com`
3. Database: `postgres`
4. Authentication: Azure AD (with managed identity) or Username/Password

**Option 2: OData Feed**
1. Get Data → OData Feed
2. URL: `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/powerbi`
3. Select tables and load data

### Prometheus Scraping

Add scrape configuration to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'logicmonitor'
    scrape_interval: 60s
    metrics_path: '/metrics/prometheus'
    params:
      hours: ['1']  # Last hour of data
    static_configs:
      - targets: ['ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io']
```

## Local Development

### Setup

```bash
# Clone repository
git clone https://github.com/ryanmat/HttpIngest.git
cd HttpIngest

# Install dependencies
uv sync

# Set up local environment variables
cp local.settings.json.example local.settings.json
# Edit local.settings.json with your database credentials

# Run database migrations
uv run alembic upgrade head

# Run application locally
uv run python -m uvicorn containerapp_main:app --reload --host 0.0.0.0 --port 8000
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_config.py -v

# Run load tests
uv run locust -f tests/load/locustfile.py --host=http://localhost:8000
```

### Database Migrations

```bash
# Create new migration
uv run alembic revision --autogenerate -m "Description of changes"

# Apply migrations
uv run alembic upgrade head

# Rollback one migration
uv run alembic downgrade -1

# View migration history
uv run alembic history
```

## Monitoring

### Health Endpoint

```json
GET /api/health

Response:
{
  "status": "healthy",
  "timestamp": "2025-01-16T20:26:03.518041",
  "components": {
    "database": "healthy",
    "streaming": "not initialized",
    "background_tasks": "3/3 running"
  }
}
```

### Application Insights

Application automatically logs to Application Insights when `APPINSIGHTS_CONNECTION_STRING` is configured:
- Request/response metrics
- Exception tracking
- Custom metrics (processing rate, queue depth)
- Dependency tracking (database, external APIs)

### Container App Logs

```bash
# Stream logs
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --follow

# Query specific errors
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --tail 100 | grep ERROR
```

## Performance

**Current Production Metrics (v12.4):**
- Processing rate: 35-50 records/min per replica
- Auto-scaling: 3-10 replicas based on load
- Database pool: 10 connections per replica
- Response time: <100ms for ingestion endpoint
- Materialized view refresh: On-demand or scheduled

**Optimization Tips:**
- Enable Redis for distributed caching
- Increase `DATA_PROCESSING_INTERVAL` to batch larger amounts
- Scale replicas horizontally for increased throughput
- Use materialized views for frequently accessed aggregations
- Enable gzip compression for OTLP payloads

## Database Schema

### Resources Table
```sql
CREATE TABLE resources (
    id BIGSERIAL PRIMARY KEY,
    service_name VARCHAR(255),
    service_namespace VARCHAR(255),
    host_name VARCHAR(255),
    instance_id VARCHAR(255),
    attributes JSONB,
    UNIQUE(service_name, service_namespace, host_name, instance_id)
);
```

### Metric Data Table
```sql
CREATE TABLE metric_data (
    id BIGSERIAL PRIMARY KEY,
    metric_def_id BIGINT REFERENCES metric_definitions(id),
    resource_id BIGINT REFERENCES resources(id),
    datasource_id BIGINT REFERENCES datasources(id),
    timestamp TIMESTAMPTZ NOT NULL,
    value_double DOUBLE PRECISION,
    value_int BIGINT,
    attributes JSONB
);
CREATE INDEX idx_metric_data_timestamp ON metric_data(timestamp);
CREATE INDEX idx_metric_data_metric_def ON metric_data(metric_def_id);
```

### Materialized Views
```sql
-- Hourly aggregates
CREATE MATERIALIZED VIEW hourly_aggregates AS
SELECT
    date_trunc('hour', timestamp) AS hour,
    metric_def_id,
    resource_id,
    COUNT(*) AS count,
    AVG(COALESCE(value_double, value_int)) AS avg_value,
    MIN(COALESCE(value_double, value_int)) AS min_value,
    MAX(COALESCE(value_double, value_int)) AS max_value
FROM metric_data
GROUP BY date_trunc('hour', timestamp), metric_def_id, resource_id;

CREATE INDEX idx_hourly_aggregates_hour ON hourly_aggregates(hour);
```

## Troubleshooting

### Token Expiration
Managed identity tokens expire after 90 minutes. Application automatically refreshes every 45 minutes.

**Manual token refresh:**
```bash
az containerapp restart \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group
```

### Database Connection Issues
Check managed identity permissions:
```bash
# Verify Azure AD admin
az postgres flexible-server ad-admin list \
  --resource-group CTA_Resource_Group \
  --server-name rm-postgres
```

### Collector Publisher Not Processing
Check background task status:
```bash
# View environment variables
az containerapp show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "properties.template.containers[0].env"

# Verify ENABLE_COLLECTOR_PUBLISHER=true
```

### High Memory Usage
Adjust connection pool settings:
```python
# In containerapp_main.py or via environment variables
DATABASE_POOL_SIZE=10  # Reduce if needed
DATABASE_MAX_OVERFLOW=20
```
