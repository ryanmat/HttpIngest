# Description: Integration notes for quantum_mcp project.
# Description: Reference when working on quantum optimization layer.

# quantum_mcp Integration Notes

These notes should be applied when working on the quantum_mcp project.

## Integration Architecture

```
HttpIngest --> Precursor --> quantum_mcp
(data)        (ML)          (optimization)
```

**HttpIngest does NOT directly integrate with quantum_mcp.**

The quantum layer operates on Precursor's ML models, not raw data from HttpIngest.

## Data Flow

1. **HttpIngest** collects OTLP metrics from LogicMonitor collectors
2. **HttpIngest** stores data in Azure Data Lake Gen2 (Parquet files)
3. **Precursor** queries HttpIngest's ML endpoints for training data
4. **Precursor** trains X-DEC model on the data
5. **quantum_mcp** enhances Precursor's expert routing and clustering

## HttpIngest Changes (v32+)

HttpIngest has migrated to Data Lake only mode:

- Primary storage: Azure Data Lake Gen2 (Parquet)
- Query engine: Azure Synapse Serverless SQL
- ML endpoints: `/api/ml/*` (backed by Synapse)
- PostgreSQL hot cache: Disabled by default

## No Changes Required in quantum_mcp

Since quantum_mcp integrates with Precursor (not HttpIngest directly), no changes are required for the Data Lake migration.

The quantum layer still:
- Receives feature vectors from Precursor
- Optimizes expert routing via QAOA
- Enhances clustering via quantum kernels
- Uses D-Wave annealing for QUBO optimization

## Phase 14+ Integration Points

When implementing MoE Unification (Phase 14+):

### 1. QAOA Expert Routing
- Input: Feature vectors from Precursor (sourced from HttpIngest Data Lake)
- Output: Optimal expert selection

### 2. Quantum Kernels for DEC
- Input: Cluster assignments from Precursor
- Output: Enhanced similarity scores

### 3. Multi-Agent Consensus
- Input: Predictions from multiple experts
- Output: Aggregated prediction with confidence

## Testing Data Availability

Before running quantum optimization tests, verify HttpIngest has data:

```bash
# Check HttpIngest health
curl https://ca-cta-lm-ingest.../api/health

# Expected: synapse: healthy

# Check data inventory
curl https://ca-cta-lm-ingest.../api/ml/inventory

# Expected: non-zero total_data_points
```

## MCP Server Integration

The quantum_mcp MCP server is available for Claude Code integration:

```python
# Available quantum tools:
# - quantum_anneal: QUBO optimization
# - quantum_kernel: Quantum kernel computation
# - quantum_simulate: Circuit simulation
# - quantum_vqe: Variational Quantum Eigensolver
# - quantum_qaoa: Quantum Approximate Optimization
```

These tools operate independently of HttpIngest.
