# Description: Documents HttpIngest's role in the LM Predictive Analytics ecosystem.
# Description: Explains integration with Precursor ML platform and quantum_mcp optimization.

# HttpIngest Ecosystem Integration

This document describes how HttpIngest integrates with the broader LogicMonitor Predictive Analytics ecosystem.

## Ecosystem Overview

```
+-------------------------------------------------------------------------+
|                         DATA LAYER                                       |
|                                                                          |
|   LogicMonitor         HttpIngest              Azure Data Lake Gen2      |
|   Collectors    --->   (this project)   --->   (Parquet files)           |
|   (OTLP metrics)       /api/HttpIngest         stlmingestdatalake        |
|                                                                          |
|                        ML Query Layer:                                   |
|                        - /api/ml/inventory                               |
|                        - /api/ml/training-data                           |
|                        - /api/ml/profiles                                |
|                        - /api/ml/profile-coverage                        |
|                                                                          |
|                        Query Engine: Azure Synapse Serverless SQL        |
+-------------------------------------------------------------------------+
                                    |
            +-----------------------+-----------------------+
            |                                               |
            v                                               v
+---------------------------+               +---------------------------+
|     OBSERVABILITY         |               |         ML LAYER          |
|                           |               |                           |
| lmotel (AKS)              |               |   Precursor               |
|   - OTLP receiver         |               |   - X-DEC Model           |
|   - LogicMonitor APM      |               |   - Feature Engineering   |
|                           |               |   - Prediction API        |
| Namespace: precursor-     |               |                           |
|   platform                |               +---------------------------+
| Services: httpingest,     |
|   precursor               |
+---------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                          ML LAYER                                        |
|                                                                          |
|   Precursor (predictive-insights)                                        |
|   - Feature Engineering (windowing, normalization)                       |
|   - X-DEC Model (BiGRU-XVAE-DEC clustering)                             |
|   - Prediction API (/predict/lookup)                                     |
|   - Profiles: kubernetes | collector | cloud_compute | network | ...    |
|                                                                          |
|   Location: /Users/ryan.matuszewski/dev/repositories/ai/predictive-insights
+-------------------------------------------------------------------------+
                                    |
                                    v (Phase 14+)
+-------------------------------------------------------------------------+
|                       QUANTUM LAYER                                      |
|                                                                          |
|   quantum_mcp                                                            |
|   - QAOA routing (optimal expert selection)                              |
|   - D-Wave annealing (QUBO optimization)                                |
|   - Quantum kernels (enhanced clustering)                                |
|   - Multi-agent orchestration                                            |
|                                                                          |
|   Location: /Users/ryan.matuszewski/dev/repositories/ai/quantum_mcp      |
+-------------------------------------------------------------------------+
```

## HttpIngest's Role

HttpIngest serves as the **data ingestion and ML query layer** for the ecosystem:

### 1. Data Ingestion
- Receives OTLP-formatted metrics from LogicMonitor Collectors
- Buffers data in memory (configurable threshold: 10,000 rows)
- Writes Parquet files to Azure Data Lake Gen2
- Partitions by time (year/month/day/hour) for efficient queries

### 2. Data Storage
- **Primary**: Azure Data Lake Gen2 (Parquet files)
  - Cost-effective long-term storage
  - Columnar format optimized for analytics
  - Time-partitioned for efficient scans
- **Optional**: PostgreSQL hot cache (disabled by default)
  - For real-time dashboard queries
  - 48-hour retention window

### 3. ML Query Layer
- **Query Engine**: Azure Synapse Serverless SQL
- **Cost Model**: ~$5 per TB scanned (pay-per-query)
- **Endpoints**:
  - `/api/ml/inventory` - Available metrics and resources
  - `/api/ml/training-data` - Historical data for ML training
  - `/api/ml/profiles` - Feature profile definitions
  - `/api/ml/profile-coverage` - Coverage statistics

## Integration with Precursor

### Data Flow

```
HttpIngest Data Lake --> Synapse SQL --> Precursor DataFetcher --> Feature Pipeline --> X-DEC Model
```

### How Precursor Gets Data

Precursor calls HttpIngest's ML endpoints via HTTP:

```python
# Example: Get training data for collector profile
response = requests.get(
    "https://ca-cta-lm-ingest.../api/ml/training-data",
    params={
        "profile": "collector",
        "start_time": "2026-01-01T00:00:00Z",
        "end_time": "2026-01-25T00:00:00Z",
        "limit": 100000
    }
)
training_data = response.json()["data"]
```

### Data Lake Schema

Parquet files contain these columns:

| Column | Type | Description |
|--------|------|-------------|
| resource_hash | string | SHA256 hash of resource attributes |
| datasource_name | string | LogicMonitor DataSource name |
| metric_name | string | Metric identifier |
| timestamp | timestamp | Data point timestamp (UTC) |
| value_double | float64 | Numeric value (double precision) |
| value_int | int64 | Numeric value (integer) |
| attributes | string | JSON-encoded metric attributes |
| ingested_at | timestamp | When data was ingested |
| year | int16 | Partition key |
| month | int8 | Partition key |
| day | int8 | Partition key |
| hour | int8 | Partition key |

### Feature Profiles

Precursor expects metrics matching these profiles:

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

### Integration Architecture

```
HttpIngest --> Precursor --> quantum_mcp
(data)        (ML)          (optimization)
```

HttpIngest does NOT directly integrate with quantum_mcp. The quantum layer operates on Precursor's ML models, not raw data.

### Future Integration Points

1. **QAOA Expert Routing**: Optimal selection of ML experts using quantum optimization
2. **Quantum Kernels**: Enhanced similarity computation for DEC clustering
3. **Multi-Agent Consensus**: Quantum-enhanced prediction aggregation

## Configuration

### Data Lake Connection

HttpIngest uses Azure managed identity:

```bash
# Environment variables
USE_MANAGED_IDENTITY=true
DATALAKE_ACCOUNT=stlmingestdatalake
DATALAKE_FILESYSTEM=metrics
DATALAKE_BASE_PATH=otlp
```

### Synapse Connection

```bash
SYNAPSE_ENABLED=true
SYNAPSE_SERVER=syn-lm-analytics-ondemand.sql.azuresynapse.net
SYNAPSE_DATABASE=master
```

### RBAC Requirements

| Principal | Resource | Role |
|-----------|----------|------|
| Container App MI | Data Lake | Storage Blob Data Contributor |
| Container App MI | Synapse | Synapse SQL Administrator |
| Container App MI | Storage Account | Storage Blob Data Reader |

## Development Workflow

### Local Testing

1. Run HttpIngest locally (uses DefaultAzureCredential for auth)
2. Collectors send data to local endpoint
3. Data written to Data Lake
4. Query via Synapse or ML endpoints
5. Run Precursor against ML endpoints

### Production Testing

1. HttpIngest deployed to Azure Container Apps
2. Multiple collectors send infrastructure metrics
3. Data Lake accumulates Parquet files
4. Precursor trains on production data via `/api/ml/training-data`
5. Predictions served via Precursor API
6. LogicMonitor DataSource polls predictions

## Related Documentation

### HttpIngest
- [API Documentation](./api-documentation.md)
- [Deployment Guide](./deployment.md)
- [TODO](./todo.md)

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
| v32+ | Phase 9+ | Phase 3+ | Data Lake architecture with ML endpoints |

## Contact

- **HttpIngest**: This repository
- **Precursor**: /Users/ryan.matuszewski/dev/repositories/ai/predictive-insights
- **quantum_mcp**: /Users/ryan.matuszewski/dev/repositories/ai/quantum_mcp
