![](https://img.shields.io/badge/Code-Python-informational?style=flat&logo=python&color=ffe333&logoColor=ffffff)
![](https://img.shields.io/badge/Database-PostgreSQL-informational?style=flat&logo=postgresql&color=4169E1&logoColor=ffffff)
![](https://img.shields.io/badge/Cloud-Azure-informational?style=flat&logo=microsoftazure&color=0078D4&logoColor=ffffff)
![](https://img.shields.io/badge/Container-Docker-informational?style=flat&logo=docker&color=2496ED&logoColor=ffffff)

# LogicMonitor OTLP Data Pipeline

Async data pipeline for ingesting, normalizing, and exporting OTLP formatted JSON metrics from LogicMonitor Data Publisher(HTTPS). Serves as the **data ingestion layer** for the Precursor predictive ML ecosystem.

## Ecosystem Overview

HttpIngest is part of a three-layer ML ecosystem for predictive monitoring:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER (this project)                       │
│                                                                         │
│   LogicMonitor         HttpIngest              PostgreSQL               │
│   Collectors    ───►   (Container App)   ───►  (normalized schema)      │
│   (OTLP metrics)       /api/HttpIngest         metric_data, resources   │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          ML LAYER                                       │
│                                                                         │
│   Precursor (predictive-insights)                                       │
│   - Feature engineering (windowing, normalization)                      │
│   - X-DEC Model (BiGRU-XVAE-DEC clustering)                            │
│   - Prediction API                                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼ (Phase 14+)
┌─────────────────────────────────────────────────────────────────────────┐
│                       QUANTUM LAYER                                     │
│                                                                         │
│   quantum_mcp                                                           │
│   - QAOA routing for expert selection                                   │
│   - D-Wave annealing for QUBO optimization                             │
│   - Quantum kernels for enhanced clustering                             │
└─────────────────────────────────────────────────────────────────────────┘
```

See [docs/ecosystem-integration.md](docs/ecosystem-integration.md) for full integration details.

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
    → Precursor ML (training data queries)
```

**Key Features:**
- Async processing with asyncpg connection pooling
- Normalized database schema (resources, datasources, metrics, data points)
- Materialized views for aggregation (hourly, latest, resource summaries)
- Multiple export formats (Prometheus, Grafana SimpleJSON, PowerBI OData, CSV/JSON)
- Managed identity authentication (no password storage)
- Auto-scaling (3-10 replicas based on load)
- Gzip compression support

## Prerequisites

- Azure subscription with Container Apps and PostgreSQL Flexible Server
- LogicMonitor account with Collector HTTPS Publisher enabled
- Azure CLI (`az`) version 2.50+
- Docker (for local development)
- Python 3.12+ with `uv` package manager

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
POSTGRES_HOST=postgres-server.postgres.database.azure.com
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=managed-identity-principal-name
USE_MANAGED_IDENTITY=true

# Application
ENVIRONMENT=production
```

### Optional

```bash
# Background Tasks
DATA_PROCESSING_INTERVAL=30  # seconds
HEALTH_MONITORING_INTERVAL=60  # seconds

# Application Insights
APPINSIGHTS_CONNECTION_STRING=InstrumentationKey=...

# Security
REQUIRE_HTTPS=true
CORS_ORIGINS=*
```

## Deployment

### Manual Deployment

Use the deployment script for Container Apps:

```bash
# Deploy specific version
./scripts/deploy.sh v15 main

# Script will:
# - Build Docker image in ACR from GitHub
# - Get fresh Azure AD token
# - Deploy to Container App
# - Run health checks
```

### Manual ACR Build

```bash
# Set variables
RESOURCE_GROUP="resource-group-name"
CONTAINER_APP="container-app-name"
ACR_NAME="registry-name"
IMAGE_NAME="lm-http-ingest"
VERSION="v15"

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
  --revision-suffix v15
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
  --server-name postgres-server \
  --display-name $CONTAINER_APP \
  --object-id $PRINCIPAL_ID
```

## API Endpoints

### Ingestion

```bash
# Ingest OTLP metrics (uncompressed)
curl -X POST https://your-app.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d @otlp_payload.json

# Ingest OTLP metrics (gzip compressed)
curl -X POST https://your-app.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @otlp_payload.json.gz
```

### Health and Monitoring

```bash
# Health check
curl https://your-app.azurecontainerapps.io/api/health
```

### Export Endpoints

```bash
# Prometheus format (for scraping)
curl "https://your-app.azurecontainerapps.io/metrics"

# CSV export (for Excel/spreadsheets)
curl "https://your-app.azurecontainerapps.io/export/csv?start_time=2025-01-01T00:00:00&end_time=2025-01-02T00:00:00" -o metrics.csv

# JSON export (for custom applications)
curl "https://your-app.azurecontainerapps.io/export/json?start_time=2025-01-01T00:00:00&limit=1000"

# PowerBI OData format (native PowerBI support)
curl "https://your-app.azurecontainerapps.io/export/powerbi?skip=0&top=1000"
```

**Export Parameters:**
- `start_time` - ISO 8601 timestamp (default: 24 hours ago)
- `end_time` - ISO 8601 timestamp (default: now)
- `limit` - Maximum records to return (JSON/CSV)
- `skip` - Records to skip for pagination (PowerBI)
- `top` - Records per page (PowerBI, default: 1000)

### Grafana SimpleJSON Datasource

```bash
# Search metrics (returns list of available metrics)
curl https://your-app.azurecontainerapps.io/grafana/search

# Query time series
curl -X POST https://your-app.azurecontainerapps.io/grafana/query \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [{"target": "cpu.usage"}],
    "range": {
      "from": "2025-01-01T00:00:00Z",
      "to": "2025-01-01T23:59:59Z"
    }
  }'
```

## LogicMonitor Configuration

Configure Collector HTTPS Publisher in LogicMonitor:

```properties
# In Collector agent.conf or via UI
publisher.http.enable=true
publisher.http.url=https://your-app.azurecontainerapps.io/api/HttpIngest
publisher.http.format=otlp
publisher.http.compression=gzip
publisher.http.batch.size=100
publisher.http.batch.interval=30
```

Restart collector after configuration changes.

## Integration

### Grafana Setup

1. Install SimpleJSON datasource plugin
2. Add datasource with URL: `https://your-app.azurecontainerapps.io/grafana`
3. Create dashboard and select metrics from datasource

### PowerBI Connection

**Option 1: OData REST API (Recommended)**

The `/export/powerbi` endpoint returns OData-compatible JSON that PowerBI can consume natively:

```json
{
  "value": [
    {
      "metric": "cpu.usage",
      "timestamp": "2025-01-01T12:00:00+00:00",
      "value": 45.2,
      "datasource": "Host_CPU",
      "resource_hostId": "1234",
      "resource_hostName": "server01.example.com",
      "attr_wildAlias": "CPU_Total",
      "attr_datapointid": "5678"
    }
  ],
  "@odata.count": 10000,
  "@odata.nextLink": "/export/powerbi?skip=1000&top=1000"
}
```

PowerBI Setup:
1. Get Data → Web → URL: `https://your-app.azurecontainerapps.io/export/powerbi`
2. Transform data: Navigate to `value` column and expand records
3. Set up scheduled refresh for automatic updates

**Option 2: Direct PostgreSQL Connection**
1. Get Data → PostgreSQL Database
2. Server: `postgres-server.postgres.database.azure.com`
3. Database: `postgres`
4. Authentication: Azure AD

### Prometheus Scraping

Add scrape configuration to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'logicmonitor'
    scrape_interval: 60s
    metrics_path: '/metrics'
    static_configs:
      - targets: ['your-app.azurecontainerapps.io']
```

## Local Development

### Setup

```bash
# Clone repository
git clone https://github.com/your-org/HttpIngest.git
cd HttpIngest

# Install dependencies
uv sync

# Copy environment example
cp .env.example .env
# Edit .env with your database credentials

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
uv run pytest tests/test_exporters.py -v

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
  "version": "14.0.0",
  "components": {
    "database": "healthy",
    "background_tasks": "3/3 running"
  }
}
```

### Application Insights

Application automatically logs to Application Insights when `APPINSIGHTS_CONNECTION_STRING` is configured:
- Request/response metrics
- Exception tracking
- Custom metrics (processing rate, queue depth)
- Dependency tracking (database)

### Container App Logs

```bash
# Stream logs
az containerapp logs show \
  --name container-app-name \
  --resource-group resource-group-name \
  --follow

# Query specific errors
az containerapp logs show \
  --name container-app-name \
  --resource-group resource-group-name \
  --tail 100 | grep ERROR
```

## Performance

**Current Production Metrics:**
- Processing rate: 35-50 records/min per replica
- Auto-scaling: 3-10 replicas based on load
- Database pool: 5-20 connections per replica
- Response time: <100ms for ingestion endpoint
- Materialized view refresh: On-demand or scheduled

**Optimization Tips:**
- Increase batch size for higher throughput
- Scale replicas horizontally for increased load
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
    metric_definition_id BIGINT REFERENCES metric_definitions(id),
    resource_id BIGINT REFERENCES resources(id),
    datasource_id BIGINT REFERENCES datasources(id),
    timestamp TIMESTAMPTZ NOT NULL,
    value_double DOUBLE PRECISION,
    value_int BIGINT,
    attributes JSONB
);
CREATE INDEX idx_metric_data_timestamp ON metric_data(timestamp);
CREATE INDEX idx_metric_data_metric_definition ON metric_data(metric_definition_id);
```

### Materialized Views
```sql
CREATE MATERIALIZED VIEW hourly_aggregates AS
SELECT
    date_trunc('hour', timestamp) AS hour,
    metric_definition_id,
    resource_id,
    COUNT(*) AS count,
    AVG(COALESCE(value_double, value_int)) AS avg_value,
    MIN(COALESCE(value_double, value_int)) AS min_value,
    MAX(COALESCE(value_double, value_int)) AS max_value
FROM metric_data
GROUP BY date_trunc('hour', timestamp), metric_definition_id, resource_id;

CREATE INDEX idx_hourly_aggregates_hour ON hourly_aggregates(hour);
```

## Project Structure

```
.
├── containerapp_main.py      # FastAPI application entry point
├── Dockerfile.containerapp   # Container image build file
├── pyproject.toml           # Python dependencies (uv)
├── uv.lock                  # Locked dependencies
├── alembic.ini             # Database migration config
├── alembic/                # Database migrations
│   └── versions/
├── src/                    # Source code
│   ├── data_processor_async.py  # Async data processing
│   ├── exporters.py        # Export format handlers
│   └── otlp_parser.py      # OTLP parsing logic
├── scripts/                # Deployment and utility scripts
│   ├── deploy.sh           # Automated deployment
│   └── migrate.py          # Migration helper
├── tests/                  # Test suite
│   ├── conftest.py
│   ├── fixtures/
│   ├── load/
│   └── test_*.py
└── docs/                   # Documentation
    ├── api-documentation.md
    ├── deployment.md
    ├── ecosystem-integration.md  # ML ecosystem integration
    ├── migrations.md
    └── otlp_parser.md
```

## Troubleshooting

### Token Expiration
Managed identity tokens expire after 90 minutes. Application automatically refreshes every 45 minutes.

**Manual token refresh:**
```bash
az containerapp restart \
  --name container-app-name \
  --resource-group resource-group-name
```

### Database Connection Issues
Check managed identity permissions:
```bash
# Verify Azure AD admin
az postgres flexible-server ad-admin list \
  --resource-group resource-group-name \
  --server-name postgres-server
```

### Background Task Issues
Check background task status:
```bash
# View environment variables
az containerapp show \
  --name container-app-name \
  --resource-group resource-group-name \
  --query "properties.template.containers[0].env"
```

### High Memory Usage
Adjust connection pool settings in containerapp_main.py:
```python
# Database pool configuration
db_pool = await asyncpg.create_pool(
    **db_params,
    min_size=5,    # Reduce if needed
    max_size=20,   # Reduce if needed
    command_timeout=60
)
```

## License

See LICENSE file for details.
