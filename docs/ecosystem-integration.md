# Description: Documents HttpIngest's role in the LM Predictive Analytics ecosystem.
# Description: Explains integration with Precursor ML platform and quantum_mcp optimization.

# HttpIngest Ecosystem Integration

This document describes how HttpIngest integrates with the broader LogicMonitor Predictive Analytics ecosystem.

## Ecosystem Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                       │
│                                                                          │
│   LogicMonitor         HttpIngest              PostgreSQL                │
│   Collectors    ───►   (this project)   ───►   (normalized schema)       │
│   (OTLP metrics)       /api/HttpIngest         metric_data, resources    │
│                                                                          │
│                        ML Endpoints:                                     │
│                        - /api/ml/inventory                               │
│                        - /api/ml/quality                                 │
│                        - /api/ml/training-data                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          ML LAYER                                        │
│                                                                          │
│   Precursor (predictive-insights)                                        │
│   ├── Feature Engineering (windowing, normalization)                     │
│   ├── X-DEC Model (BiGRU-XVAE-DEC clustering)                           │
│   ├── Prediction API (/predict/lookup)                                   │
│   └── Profiles: kubernetes | collector | cloud_compute | network | ...   │
│                                                                          │
│   Location: /Users/ryan.matuszewski/dev/repositories/ai/predictive-insights
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Phase 14+)
┌─────────────────────────────────────────────────────────────────────────┐
│                       QUANTUM LAYER                                      │
│                                                                          │
│   quantum_mcp                                                            │
│   ├── QAOA routing (optimal expert selection)                            │
│   ├── D-Wave annealing (QUBO optimization)                              │
│   ├── Quantum kernels (enhanced clustering)                              │
│   └── Multi-agent orchestration                                          │
│                                                                          │
│   Location: /Users/ryan.matuszewski/dev/repositories/ai/quantum_mcp      │
└─────────────────────────────────────────────────────────────────────────┘
```

## HttpIngest's Role

HttpIngest serves as the **data ingestion and serving layer** for the ML ecosystem:

### 1. Data Ingestion
- Receives OTLP-formatted metrics from LogicMonitor Collectors
- Normalizes into PostgreSQL schema (resources, datasources, metric_definitions, metric_data)
- Handles batch processing with async workers

### 2. Data Export (Current)
- Prometheus format (`/metrics`)
- Grafana SimpleJSON (`/grafana/*`)
- PowerBI OData (`/export/powerbi`)
- CSV/JSON (`/export/csv`, `/export/json`)

### 3. ML Data Service (Live - v22+)
- Metrics inventory (`/api/ml/inventory`)
- Profile coverage (`/api/ml/profile-coverage`)
- Training data streaming (`/api/ml/training-data`)
- Data quality metrics (`/api/ml/quality`)

## Integration with Precursor

### Data Flow

```
HttpIngest PostgreSQL ──► Precursor DataFetcher ──► Feature Pipeline ──► X-DEC Model
```

### What Precursor Queries

Precursor's `DataFetcher` executes this query against HttpIngest's database:

```sql
SELECT
    r.id AS resource_id,
    r.attributes->>'hostName' AS host_name,
    md.name AS metric_name,
    m.timestamp,
    COALESCE(m.value_double, m.value_int::float) AS value,
    m.attributes->>'dataSourceInstanceName' AS datasource_instance
FROM metric_data m
JOIN resources r ON m.resource_id = r.id
JOIN metric_definitions md ON m.metric_definition_id = md.id
WHERE m.timestamp >= $1 AND m.timestamp <= $2
ORDER BY r.id, m.timestamp, md.name
```

### Schema Requirements

HttpIngest must provide these tables:

| Table | Required Columns |
|-------|------------------|
| `metric_data` | id, resource_id, metric_definition_id, timestamp, value_double, value_int, attributes |
| `resources` | id, attributes (JSONB with hostName, displayName) |
| `metric_definitions` | id, name |

### Feature Profiles

Precursor expects metrics matching these profiles (defined in `config/features.yaml`):

| Profile | Use Case | Example Metrics |
|---------|----------|-----------------|
| kubernetes | Container workloads | cpuUsageNanoCores, memoryUsageBytes |
| collector | LM Collector monitoring | ExecuteTime, ThreadCount, CpuUsage |
| cloud_compute | AWS/Azure VMs | cpuUtilization, memoryUtilization |
| network | SNMP devices | ifInOctets, ifOutOctets |
| database | SQL/NoSQL servers | activeConnections, queryExecutionTime |
| application | APM metrics | requestRate, errorRate, responseTime |

## Integration with quantum_mcp

quantum_mcp integration occurs at **Precursor Phase 14+** (MoE Unification).

### Future Integration Points

1. **QAOA Expert Routing**: Optimal selection of ML experts using quantum optimization
2. **Quantum Kernels**: Enhanced similarity computation for DEC clustering
3. **Multi-Agent Consensus**: Quantum-enhanced prediction aggregation

HttpIngest does not directly integrate with quantum_mcp. The quantum layer operates on Precursor's ML models, not raw data.

## Configuration

### Database Connection

Both HttpIngest and Precursor share the same PostgreSQL database:

```bash
# Environment variables (shared)
PGHOST=rm-postgres.postgres.database.azure.com
PGPORT=5432
PGDATABASE=postgres
PGUSER=ryan.matuszewski@logicmonitor.com
PGPASSWORD=$(az account get-access-token --resource https://ossrdbms-aad.database.windows.net --query accessToken -o tsv)
```

### Azure Managed Identity

In production (Azure Container Apps), both services use managed identity:

```bash
USE_MANAGED_IDENTITY=true
# Token refresh handled automatically
```

## Development Workflow

### Local Testing

1. Start HttpIngest (receives collector self-monitoring metrics)
2. Data accumulates in PostgreSQL
3. Run Precursor feature extraction against local DB
4. Train X-DEC model on collector profile
5. Validate predictions

### Production Testing

1. HttpIngest deployed to Azure Container Apps
2. Multiple collectors send infrastructure metrics
3. Precursor trains on production data volume (target: 56M+ rows)
4. Predictions served via Precursor API
5. LogicMonitor DataSource polls predictions

## Related Documentation

### HttpIngest
- [API Documentation](./api-documentation.md)
- [Deployment Guide](./deployment.md)
- [Database Migrations](./migrations.md)

### Precursor
- `/docs/roadmap.md` - Phase roadmap (9-16)
- `/docs/prediction_datasource_spec.md` - LM DataSource spec
- `/docs/integration_with_quantum_mcp.md` - Quantum integration

### quantum_mcp
- `/docs/plan.md` - Implementation phases
- `/docs/integration_with_predictive_insights.md` - Precursor integration
- `/docs/quantum_backends.md` - Backend specifications

## Version Compatibility

| HttpIngest | Precursor | quantum_mcp | Notes |
|------------|-----------|-------------|-------|
| v22+ | Phase 9+ | Phase 3+ | Current compatible versions (ML endpoints live) |

## Contact

- **HttpIngest**: This repository
- **Precursor**: /Users/ryan.matuszewski/dev/repositories/ai/predictive-insights
- **quantum_mcp**: /Users/ryan.matuszewski/dev/repositories/ai/quantum_mcp
