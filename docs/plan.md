# Description: Implementation roadmap for HttpIngest ML integration.
# Description: Tracks phases from data layer to full ML service capabilities.

# HttpIngest Implementation Plan

This document outlines the implementation roadmap for enhancing HttpIngest to serve as the data layer for the LM Predictive Analytics ML ecosystem.

## Current State (v14)

HttpIngest currently provides:
- OTLP metric ingestion from LogicMonitor Collectors
- Normalized PostgreSQL schema (resources, datasources, metric_definitions, metric_data)
- Export endpoints (Prometheus, Grafana, PowerBI, CSV, JSON)
- Azure Container Apps deployment with managed identity

## Target State

HttpIngest will provide ML-ready data services for Precursor:
- Metrics inventory API for profile discovery
- Training data streaming endpoint
- Data quality metrics
- Profile coverage reporting

## Implementation Phases

### Phase A: Core ML Endpoints (Current Focus)

**Goal**: Provide minimal endpoints needed for Precursor integration.

| Task | Description | Status |
|------|-------------|--------|
| A.1 | Add `/api/ml/inventory` endpoint | Pending |
| A.2 | Add `/api/ml/training-data` streaming endpoint | Pending |
| A.3 | Add `/api/ml/profile-coverage` endpoint | Pending |
| A.4 | Document profile-to-metrics mapping | Pending |

**Endpoints**:
```
GET /api/ml/inventory
    Returns: Available metrics, resources, time ranges

GET /api/ml/training-data?profile=collector&start=7d&end=now
    Returns: Streaming training data in format Precursor expects

GET /api/ml/profile-coverage?profile=collector
    Returns: Coverage statistics for requested profile
```

### Phase B: Data Quality

**Goal**: Ensure data quality for ML training.

| Task | Description | Status |
|------|-------------|--------|
| B.1 | Add `/api/ml/quality` endpoint | Pending |
| B.2 | Implement gap detection | Pending |
| B.3 | Add data freshness metrics | Pending |
| B.4 | Table partitioning for large datasets | Pending |

### Phase C: Integration Testing

**Goal**: Validate end-to-end integration with Precursor.

| Task | Description | Status |
|------|-------------|--------|
| C.1 | Local integration tests | Pending |
| C.2 | Production data testing (56M+ rows) | Pending |
| C.3 | Performance benchmarking | Pending |
| C.4 | Documentation completion | Pending |

## Ecosystem Integration

HttpIngest integrates with:

| Project | Role | Integration Point |
|---------|------|-------------------|
| **Precursor** | ML Training | PostgreSQL queries, ML API endpoints |
| **quantum_mcp** | Quantum Optimization | No direct integration (via Precursor) |

See [ecosystem-integration.md](./ecosystem-integration.md) for full details.

## Feature Profiles

HttpIngest should support these Precursor feature profiles:

| Profile | Target Metrics | Use Case |
|---------|---------------|----------|
| `collector` | ExecuteTime, ThreadCount, CpuUsage, SuccessRate | LM Collector monitoring |
| `kubernetes` | cpuUsageNanoCores, memoryUsageBytes | Container workloads |
| `cloud_compute` | cpuUtilization, memoryUtilization | AWS/Azure VMs |
| `network` | ifInOctets, ifOutOctets | SNMP devices |
| `database` | activeConnections, queryExecutionTime | SQL/NoSQL servers |
| `application` | requestRate, errorRate, responseTime | APM metrics |

## Dependencies

- PostgreSQL schema must include: metric_data, resources, metric_definitions
- Precursor DataFetcher expects specific JSONB attribute structure
- Managed identity for Azure PostgreSQL authentication

## Related Documentation

- [ecosystem-integration.md](./ecosystem-integration.md) - Full ecosystem overview
- [api-documentation.md](./api-documentation.md) - API reference
- [migrations.md](./migrations.md) - Database schema changes

## Version History

| Version | Changes |
|---------|---------|
| v14 | Cleanup, removed dead code, normalized schema migrations |
| v15 (planned) | ML endpoints Phase A |
