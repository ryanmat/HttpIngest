![](https://img.shields.io/badge/Code-Python-informational?style=flat&logo=python&color=ffe333&logoColor=ffffff)
![](https://img.shields.io/badge/Storage-Azure_Data_Lake-informational?style=flat&logo=microsoftazure&color=0078D4&logoColor=ffffff)
![](https://img.shields.io/badge/Cloud-Azure-informational?style=flat&logo=microsoftazure&color=0078D4&logoColor=ffffff)
![](https://img.shields.io/badge/Container-Docker-informational?style=flat&logo=docker&color=2496ED&logoColor=ffffff)

# LogicMonitor OTLP Data Pipeline

Async data pipeline for ingesting, normalizing, and exporting OTLP formatted JSON metrics from LogicMonitor Data Publisher (HTTPS). Serves as the **data ingestion layer** for the Precursor predictive ML ecosystem.

## Ecosystem Overview

HttpIngest is the data layer of a three-tier ML ecosystem for predictive monitoring:

```
                         DATA LAYER (this project)
+-------------------------------------------------------------------------+
|                                                                         |
|   LogicMonitor         HttpIngest              Azure Data Lake Gen2     |
|   Collectors    --->   (Container App)   --->  (Parquet files)          |
|   (OTLP metrics)       /api/HttpIngest         stlmingestdatalake       |
|                                                                         |
+-------------------------------------------------------------------------+
                                   |
                                   | ML Query Layer (/api/ml/*)
                                   v
+-------------------------------------------------------------------------+
|                          ML LAYER                                       |
|                                                                         |
|   Precursor (precursor)                                       |
|   - Feature engineering (windowing, normalization)                      |
|   - X-DEC Model (BiGRU-XVAE-DEC clustering)                            |
|   - Prediction API                                                      |
+-------------------------------------------------------------------------+
                                   |
                                   v (Phase 14+)
+-------------------------------------------------------------------------+
|                       QUANTUM LAYER                                     |
|                                                                         |
|   quantum_mcp                                                           |
|   - QAOA routing for expert selection                                   |
|   - D-Wave annealing for QUBO optimization                             |
|   - Quantum kernels for enhanced clustering                             |
+-------------------------------------------------------------------------+
```

See [docs/ecosystem-integration.md](docs/ecosystem-integration.md) for full integration details.

## Architecture

**Storage Mode:** Data Lake only (v32+)

**Components:**
- Azure Container Apps (async Python FastAPI)
- Azure Data Lake Gen2 (Parquet files, partitioned by time)
- Azure Synapse Serverless SQL (ML query layer)
- Azure Managed Identity (passwordless auth)

**Data Flow:**
```
LogicMonitor Collector --> HTTPS Publisher --> Container App (/api/HttpIngest)
    --> Data Lake (Parquet, year/month/day/hour partitions)
    --> Synapse Serverless SQL (/api/ml/* endpoints)
    --> Precursor ML (training data)
```

**Key Features:**
- Async processing with buffered writes
- Parquet files with time-based partitioning (year/month/day/hour)
- Synapse Serverless SQL for cost-effective ML queries (~$5/TB scanned)
- Managed identity authentication (no password storage)
- Auto-scaling (1-5 replicas based on HTTP concurrency)
- Gzip compression support

## Prerequisites

- Azure subscription with Container Apps and Data Lake Gen2
- LogicMonitor account with Collector HTTPS Publisher enabled
- Azure CLI (`az`) version 2.50+
- Python 3.12+ with `uv` package manager

## Quick Start

```bash
# Clone repository
git clone https://github.com/ryanmat/HttpIngest.git
cd HttpIngest

# Install dependencies
uv sync

# Run locally (requires Azure credentials)
uv run python -m uvicorn containerapp_main:app --reload --host 0.0.0.0 --port 8000
```

## Environment Variables

**Required (set on Container App):**
```bash
USE_MANAGED_IDENTITY=true           # Use Azure managed identity
HOT_CACHE_ENABLED=false             # Disable PostgreSQL hot cache
SYNAPSE_ENABLED=true                # Enable Synapse ML queries
ENABLE_COLLECTOR_PUBLISHER=true     # Enable collector data ingestion
```

**Data Lake Configuration:**
```bash
DATALAKE_ACCOUNT=stlmingestdatalake       # Storage account name
DATALAKE_FILESYSTEM=metrics               # Container name
DATALAKE_BASE_PATH=otlp                   # Base path in container
DATALAKE_FLUSH_INTERVAL_SECONDS=60        # Buffer flush interval
DATALAKE_FLUSH_THRESHOLD_ROWS=10000       # Flush when buffer hits this size
```

**Synapse Configuration:**
```bash
SYNAPSE_SERVER=syn-lm-analytics-ondemand.sql.azuresynapse.net
SYNAPSE_DATABASE=master
```

## Deployment

### Quick Deploy

```bash
# 1. Build image in ACR
az acr build --registry acrctalmhttps001 \
  --image httpingest:v32 \
  --file Dockerfile.containerapp .

# 2. Deploy to Container App
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --image acrctalmhttps001.azurecr.io/httpingest:v32

# 3. Verify health
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
```

### Automated Deploy

```bash
./scripts/deploy.sh v32
```

See [docs/deployment.md](docs/deployment.md) for full deployment guide.

## API Endpoints

### Ingestion

```bash
# Ingest OTLP metrics
curl -X POST https://your-app.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d @otlp_payload.json

# With gzip compression
curl -X POST https://your-app.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @otlp_payload.json.gz
```

### Health

```bash
curl https://your-app.azurecontainerapps.io/api/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "32.0.0",
  "mode": "datalake_only",
  "components": {
    "datalake": { "status": "healthy" },
    "hot_cache": { "status": "disabled" },
    "synapse": { "status": "healthy" }
  }
}
```

### ML Endpoints (via Synapse)

```bash
# Get data inventory
curl https://your-app.azurecontainerapps.io/api/ml/inventory

# Get training data
curl "https://your-app.azurecontainerapps.io/api/ml/training-data?start_time=2026-01-01T00:00:00Z&end_time=2026-01-25T00:00:00Z"

# List feature profiles
curl https://your-app.azurecontainerapps.io/api/ml/profiles

# Check profile coverage
curl https://your-app.azurecontainerapps.io/api/ml/profile-coverage
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

## Data Lake Structure

Parquet files are organized by time partitions:

```
metrics/
  otlp/
    metric_data/
      year=2026/
        month=01/
          day=25/
            hour=12/
              part-20260125120000-abc123.parquet
    resources/
      resources-20260125120000-def456.parquet
    datasources/
      datasources-20260125120000-ghi789.parquet
    metric_definitions/
      metric_definitions-20260125120000-jkl012.parquet
```

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_datalake_components.py -v
```

## Project Structure

```
.
├── containerapp_main.py      # FastAPI application entry point
├── Dockerfile.containerapp   # Container image build file
├── pyproject.toml           # Python dependencies (uv)
├── src/                     # Source code
│   ├── datalake_writer.py   # Data Lake Parquet writer
│   ├── synapse_client.py    # Synapse Serverless SQL client
│   ├── ml_service.py        # ML endpoint service
│   ├── ingestion_router.py  # Route data to storage backends
│   ├── otlp_parser.py       # OTLP parsing logic
│   └── exporters.py         # Export format handlers
├── scripts/                 # Deployment scripts
│   └── deploy.sh            # Automated deployment
├── tests/                   # Test suite
└── docs/                    # Documentation
    ├── deployment.md
    ├── ecosystem-integration.md
    └── api-documentation.md
```

## Monitoring

### Container App Logs

```bash
# Stream logs
az containerapp logs show --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group --follow

# Check for errors
az containerapp logs show --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group --tail 50 | grep ERROR
```

### Scaling

```bash
# Current: 1-5 replicas, scales on HTTP concurrency (10 per replica)
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --min-replicas 1 --max-replicas 10
```

## Troubleshooting

### Empty ML Inventory
- Verify collectors are sending data to `/api/HttpIngest`
- Check Data Lake buffer is flushing (health endpoint shows buffer stats)
- Confirm Synapse has Storage Blob Data Reader role on Data Lake

### Synapse Connection Errors
- Verify managed identity has Synapse SQL Administrator role
- Check Synapse firewall allows Azure services (0.0.0.0-0.0.0.0)

### Data Not Appearing
- Default flush interval is 60 seconds, or 10,000 rows
- Check container logs for flush activity: `grep flush`

## License

See LICENSE file for details.
