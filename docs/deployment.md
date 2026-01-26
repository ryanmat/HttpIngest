# Description: Deployment guide for HttpIngest Azure Container App.
# Description: Covers building, deploying, and managing the Data Lake ingestion pipeline.

# Deployment Guide

**Production System:** Azure Container App receiving live LogicMonitor OTLP data

## Overview

HttpIngest runs in **Data Lake only mode** - all OTLP metrics are written to Azure Data Lake Gen2 as Parquet files. No PostgreSQL database is required.

## Infrastructure

**Azure Resources:**
- **Container Registry:** `acrctalmhttps001`
- **Container App:** `ca-cta-lm-ingest`
- **Resource Group:** `CTA_Resource_Group`
- **Data Lake:** `stlmingestdatalake` (Azure Data Lake Gen2)
- **Current Version:** v29

**Endpoints:**
- Health: `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health`
- Ingest: `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest`

## Quick Deploy

```bash
# 1. Build image in ACR
az acr build --registry acrctalmhttps001 \
  --image httpingest:v30 \
  --file Dockerfile.containerapp .

# 2. Deploy to Container App
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --image acrctalmhttps001.azurecr.io/httpingest:v30

# 3. Verify health
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
```

## Environment Variables

**Required (set on Container App):**
- `HOT_CACHE_ENABLED=false` - Disables PostgreSQL hot cache
- `SYNAPSE_ENABLED=false` - Disables Synapse analytics
- `ENABLE_COLLECTOR_PUBLISHER=true` - Enables LogicMonitor collector data

**Data Lake Configuration (managed by Azure):**
- Uses managed identity for authentication
- Account: `stlmingestdatalake`
- Container: Configured via DataLakeConfig

## OpenTelemetry Tracing

HttpIngest supports distributed tracing via OpenTelemetry with LogicMonitor APM integration.

**Environment Variables:**
```bash
OTEL_TRACING_ENABLED=true           # Enable/disable tracing
OTEL_SERVICE_NAME=httpingest        # Service name in traces
OTEL_EXPORTER_TYPE=logicmonitor     # logicmonitor, otlp, or console
LM_ACCOUNT=your-account             # LogicMonitor account name
LM_OTEL_TOKEN=your-bearer-token     # LogicMonitor APM bearer token
OTEL_TRACES_SAMPLER_ARG=1.0         # Sampling rate (0.0-1.0)
```

**To enable in Container App:**
```bash
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars \
    OTEL_TRACING_ENABLED=true \
    OTEL_EXPORTER_TYPE=logicmonitor \
    LM_ACCOUNT=your-account \
    LM_OTEL_TOKEN=your-bearer-token
```

**Auto-instrumented:**
- FastAPI endpoints (excluding /health, /metrics)
- asyncpg database calls
- httpx HTTP client calls
- Logging (adds trace context to log messages)

## Scaling

Current configuration:
- Min replicas: 1
- Max replicas: 5
- Auto-scale on: HTTP concurrent requests (10 per replica)

To adjust:
```bash
az containerapp update --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --min-replicas 1 --max-replicas 10
```

## Health Check

```bash
curl -s https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health | jq .
```

**Expected Response:**
```json
{
  "status": "healthy",
  "mode": "datalake_only",
  "version": "22.0.0",
  "components": {
    "datalake": { "status": "healthy" },
    "hot_cache": { "status": "disabled" },
    "synapse": { "status": "disabled" }
  }
}
```

## Logs

```bash
# Stream logs
az containerapp logs show --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group --follow

# Check for errors
az containerapp logs show --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group --tail 50 | grep ERROR
```

## Rollback

```bash
# List revisions
az containerapp revision list --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group --output table

# Activate previous revision
az containerapp revision activate --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision <previous-revision-name>
```

## Troubleshooting

**Common Issues:**

1. **"pool is closed" errors** - Hot cache is enabled but token refresh breaks pool references. Solution: Set `HOT_CACHE_ENABLED=false`.

2. **"Database pool not initialized"** - Old code version requires PostgreSQL. Solution: Deploy v29+ which supports Data Lake only mode.

3. **High replica count** - Check scaling rules. Reduce max-replicas if needed.

**Check Container Status:**
```bash
az containerapp show --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "properties.runningStatus"
```

---

**Last Updated:** 2026-01-25
**Current Version:** v39 (Data Lake only mode, Synapse ML query layer, OpenTelemetry tracing)
