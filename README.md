![](https://img.shields.io/badge/Code-Python-informational?style=flat&logo=python&color=ffe333&logoColor=ffffff)
![](https://img.shields.io/badge/Storage-Azure_Data_Lake-informational?style=flat&logo=microsoftazure&color=0078D4&logoColor=ffffff)
![](https://img.shields.io/badge/Cloud-Azure-informational?style=flat&logo=microsoftazure&color=0078D4&logoColor=ffffff)
![](https://img.shields.io/badge/Container-Docker-informational?style=flat&logo=docker&color=2496ED&logoColor=ffffff)

# LogicMonitor OTLP Data Pipeline

Async data pipeline for ingesting and storing OTLP formatted JSON metrics from LogicMonitor
Data Publisher (HTTPS) into Azure Data Lake Gen2 as Parquet. Serves as the **data ingestion
layer** for the Precursor predictive ML ecosystem.

## Ecosystem Overview

HttpIngest is the ingestion layer of a multi-tier ML ecosystem for predictive monitoring.
It receives metrics and writes Parquet to ADLS. Downstream consumers read ADLS directly.

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
                      Direct ADLS reads (DuckDB, gsutil)
                                   |
              +--------------------+--------------------+
              v                                         v
+---------------------------+            +---------------------------+
|       ML LAYER            |            |    QUANTUM + PDP LAYER    |
|                           |            |                           |
|  Precursor (GCP Cloud Run)|            |  quantum_mcp              |
|  - X-DEC, Chronos, TTM   |            |  - QUBO optimization      |
|  - Training via Vertex AI |            |  - D-Wave, Qiskit, IBM    |
|  - Reads GCS Parquet      |            |  - Reads PDP tracker      |
+---------------------------+            +---------------------------+
```

## Architecture

**Current Version:** v53 (ADLS-only mode, deployed 2026-03-31)

**Components:**
- Azure Container Apps (async Python FastAPI, 0-3 replicas, scale-to-zero)
- Azure Data Lake Gen2 (Parquet files, Hive-partitioned by time)
- Azure Managed Identity (passwordless auth)

**Data Flow:**
```
LogicMonitor Collector 117 --> Data Publisher (OTLP, gzip)
    --> Container App (/api/HttpIngest)
    --> Async buffer (600s interval or 50K rows)
    --> ADLS Parquet (year/month/day/hour partitions)
    --> DuckDB direct reads (weekly training cron)
    --> gsutil cp to GCS (for Vertex AI training)
```

**Key Features:**
- Async processing with buffered writes (non-blocking ADLS uploads via asyncio.to_thread)
- Hive-partitioned Parquet files (year/month/day/hour)
- Scale-to-zero (0 replicas when idle, scales to 3 under load)
- Managed identity authentication (no password storage)
- Gzip decompression on ingest
- Prometheus /metrics endpoint

**Removed in v53 (Session 28 cleanup, -12,100 lines):**
- Azure Synapse Serverless SQL (workspace deleted, never used for training)
- ML query endpoints (/api/ml/*) -- training reads ADLS via DuckDB directly
- PostgreSQL hot cache
- Export format handlers (Grafana, PowerBI, CSV, JSON)
- ODBC drivers from Docker image (~200MB savings)

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
ENABLE_COLLECTOR_PUBLISHER=true     # Enable collector data ingestion
```

**Data Lake Configuration:**
```bash
DATALAKE_ACCOUNT=stlmingestdatalake       # Storage account name
DATALAKE_FILESYSTEM=metrics               # Container name
DATALAKE_BASE_PATH=otlp                   # Base path in container
DATALAKE_FLUSH_INTERVAL_SECONDS=600       # Buffer flush interval (10 min)
DATALAKE_FLUSH_THRESHOLD_ROWS=50000       # Flush when buffer hits this size
```

## Deployment

### Quick Deploy

```bash
# 1. Build image in ACR
az acr build --registry acrctalmhttps001 \
  --image httpingest:v53 \
  --file Dockerfile.containerapp .

# 2. Deploy to Container App
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --image acrctalmhttps001.azurecr.io/httpingest:v53

# 3. Verify health
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
```

### Automated Deploy

```bash
./scripts/deploy.sh v53
```

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
  "version": "53.0.0",
  "mode": "datalake_only",
  "components": {
    "datalake": { "status": "healthy" }
  }
}
```

### Metrics

```bash
# Prometheus-format metrics
curl https://your-app.azurecontainerapps.io/metrics
```

## LogicMonitor Configuration

Data Publisher is configured on Collector 117 in the LM portal:

```properties
publisher.http.enable=true
publisher.http.url=https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest
publisher.http.format=otlp
publisher.http.compression=gzip
publisher.http.batch.size=100
publisher.http.batch.interval=30
```

## Data Lake Structure

Parquet files are Hive-partitioned by time:

```
stlmingestdatalake/
  metrics/
    otlp/
      metric_data/
        year=2026/
          month=03/
            day=31/
              hour=12/
                part-20260331120000-abc123.parquet
```

**Downstream consumers read ADLS directly:**
- Weekly training cron: DuckDB reads Parquet partitions, uploads to GCS via gsutil
- Compaction script: `scripts/compact_parquet.py` merges small files to day-level (manual)

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
├── Dockerfile.containerapp   # Container image (non-root, single worker)
├── pyproject.toml           # Python dependencies (uv), version 53.0.0
├── src/                     # Source code
│   ├── datalake_writer.py   # Async Data Lake Parquet writer
│   ├── ingestion_router.py  # Route data to ADLS backend
│   ├── otlp_parser.py       # OTLP parsing and normalization
│   └── tracing.py           # OpenTelemetry instrumentation
├── scripts/
│   ├── deploy.sh            # Automated ACR build + Container App deploy
│   └── compact_parquet.py   # Merge small Parquet files to day-level
├── tests/                   # 10 test files
└── docs/                    # Documentation
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
# Current: 0-3 replicas, scale-to-zero enabled
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --min-replicas 0 --max-replicas 3
```

## Troubleshooting

### Data Not Appearing
- Default flush interval is 600 seconds (10 min), or 50,000 rows
- Check container logs for flush activity: `grep flush`
- Verify Data Publisher is active on Collector 117 in LM portal

### Container App Not Starting
- Check ACR image tag matches deployed revision
- Verify Managed Identity has Storage Blob Data Contributor on ADLS

### Stale Partitions
- Run compaction script to merge small hourly files: `uv run python scripts/compact_parquet.py`

## License

See LICENSE file for details.
