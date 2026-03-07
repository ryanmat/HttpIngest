# Description: Implementation roadmap for HttpIngest ML integration.
# Description: Tracks phases from data layer to full ML service capabilities.

# HttpIngest Implementation Plan

This document outlines the implementation roadmap for HttpIngest as the data layer for the LM Predictive Analytics ML ecosystem.

## Current State (v49)

HttpIngest provides:
- OTLP metric ingestion from LogicMonitor Collectors
- Azure Data Lake Gen2 primary storage (Parquet, time-partitioned)
- Azure Synapse Serverless SQL query engine for ML endpoints
- In-memory Prometheus metrics (no DB dependency)
- Root /health and detailed /api/health endpoints
- PostgreSQL hot cache (dormant, available for dashboarding if needed)
- OpenTelemetry tracing via lmotel to LogicMonitor APM

## Architecture

```
LogicMonitor Collectors (OTLP)
    -> POST /api/HttpIngest
    -> Memory buffer (flush every 60s or 10,000 rows)
    -> Azure Data Lake Gen2 (Parquet, year/month/day/hour partitions)
    -> Azure Synapse Serverless SQL (query engine)
    -> /api/ml/* endpoints
    -> Precursor (precursor) ML training
```

## Completed Phases

### Phase A: Core ML Endpoints (v15)

| Task | Description | Status |
|------|-------------|--------|
| A.1 | `/api/ml/inventory` endpoint | Done |
| A.2 | `/api/ml/training-data` endpoint | Done |
| A.3 | `/api/ml/profile-coverage` endpoint | Done (PostgreSQL-only) |
| A.4 | `/api/ml/profiles` endpoint | Done |
| A.5 | Feature profile definitions (6 profiles) | Done |

### Phase B: Data Quality (partial)

| Task | Description | Status |
|------|-------------|--------|
| B.1 | `/api/ml/quality` endpoint | Done (PostgreSQL-only) |
| B.2 | Gap detection | Not started |
| B.3 | Data freshness metrics | Not started |
| B.4 | Table partitioning | Done (Parquet time-partitioned) |

### Data Lake Migration (v32)

| Task | Description | Status |
|------|-------------|--------|
| D.1 | Data Lake Gen2 writer | Done |
| D.2 | Synapse Serverless SQL client | Done |
| D.3 | Ingestion router (dual-write support) | Done |
| D.4 | Hot cache guards on export endpoints | Done |

### Metrics and Health (v49)

| Task | Description | Status |
|------|-------------|--------|
| M.1 | In-memory Prometheus /metrics | Done |
| M.2 | Root /health for container probes | Done |
| M.3 | Export endpoint hot cache guards | Done |
| M.4 | ML feature profile alignment with Data Lake names | Done |

## Open Work

| Task | Description | Priority |
|------|-------------|----------|
| O.1 | Migrate /api/ml/profile-coverage to Synapse | Medium |
| O.2 | Migrate /api/ml/quality to Synapse | Medium |
| O.3 | Add partition pruning to /api/ml/inventory | Medium |
| O.4 | GitHub Actions CI/CD pipeline | Low |
| O.5 | Write tests for current architecture | High |

## Ecosystem Integration

| Project | Role | Integration Point |
|---------|------|-------------------|
| **Precursor** | ML Training | Synapse queries via /api/ml/* endpoints |
| **quantum_mcp** | Quantum Optimization | No direct integration (via Precursor) |

## Dependencies

- Azure Data Lake Gen2 for Parquet storage
- Azure Synapse Serverless SQL for queries (~$5/TB scanned)
- ODBC Driver 18 for Synapse connectivity
- Azure managed identity for authentication
- PostgreSQL only needed if dashboarding is enabled

## Version History

| Version | Changes |
|---------|---------|
| v14 | Cleanup, normalized schema migrations |
| v15 | ML endpoints Phase A |
| v32 | Data Lake migration, Synapse integration |
| v48 | ML profile alignment, lmotel tracing |
| v49 | In-memory metrics, /health endpoint, export guards |
