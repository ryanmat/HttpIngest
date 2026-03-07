# Description: Tracks current and upcoming tasks for HttpIngest.
# Description: Used by Claude Code to understand project state.

# HttpIngest TODO

## Current: v49 — Data Lake + Synapse

### Architecture

- Primary storage: Azure Data Lake Gen2 (stlmingestdatalake)
- Query engine: Azure Synapse Serverless SQL (~$5/TB scanned)
- Hot cache: Dormant (PostgreSQL available for dashboarding if/when needed)
- Metrics: In-memory Prometheus counters (no DB dependency)
- Health: Root /health endpoint for Azure Container Apps probes
- ML endpoints: /api/ml/* backed by Synapse for Precursor integration
- Export endpoints (Grafana, PowerBI, CSV, JSON): Return 503 without hot cache
- Replicas: 1-5 (auto-scaling on HTTP concurrency)

### Recent Completed

- [x] Replace PostgreSQL-dependent /metrics with in-memory counters (v49)
- [x] Add /health root endpoint for container probes (v49)
- [x] Guard export endpoints with hot cache checks (v49)
- [x] Align ML feature profiles with Data Lake metric names (v48)
- [x] Default tracing to lmotel OTLP endpoint (v48)
- [x] Deploy Data Lake only mode, disable PostgreSQL (v32)
- [x] Fix Synapse Serverless SQL integration (v32)

### Optional Future Work

- [ ] Enable hot cache if real-time dashboards needed (requires fixing token refresh bug)
- [ ] Migrate /api/ml/profile-coverage to Synapse backend (currently PostgreSQL-only)
- [ ] Migrate /api/ml/quality to Synapse backend (currently PostgreSQL-only)
- [ ] Add more LM metric names to profiles as discovered
- [ ] Add resource filtering to training-data endpoint

## Completed (v15 ML Integration Phase A)

- [x] Implement `/api/ml/inventory` endpoint with datasource/resource filtering
- [x] Implement `/api/ml/training-data` endpoint with profile and time range support
- [x] Implement `/api/ml/profile-coverage` endpoint with coverage percentages
- [x] Implement `/api/ml/profiles` endpoint for listing feature profiles
- [x] Add dual naming support (LM names + Precursor standard names) to profiles
- [x] Deploy v15 to Azure Container Apps
- [x] Verify profile coverage with live collector data

## Completed (v14 Cleanup)

- [x] Remove dead code (config.py, secrets.py)
- [x] Remove unused dependencies (azure-functions, requests)
- [x] Fix bare except clauses in exporters.py
- [x] Fix sys.executable issue in test_migrations.py
- [x] Consolidate duplicate DB connection logic
- [x] Update pyproject.toml with proper description
- [x] Run database migrations for normalized schema
- [x] Deploy v14 to Azure Container Apps
- [x] Create ecosystem integration documentation
- [x] Update README with ecosystem overview

## Backlog

- [ ] GitHub Actions CI/CD pipeline for auto-deploy on push to main
  - Build image in ACR on push
  - Update container app automatically
  - Currently using manual `az acr build` + `az containerapp update`
- [ ] Streaming response support for large exports
- [ ] Webhook notifications for data quality issues
- [ ] Grafana dashboard templates
- [ ] Retention policy for old metric_data

## Notes

- HttpIngest does NOT directly integrate with quantum_mcp
- Current production version: v49 (Data Lake + Synapse)
- Mode: datalake_only (PostgreSQL hot cache dormant, Synapse enabled)
- Data Lake account: stlmingestdatalake
- Synapse server: syn-lm-analytics-ondemand.sql.azuresynapse.net (enabled, query layer for Precursor)
- Replicas: 1-5 (auto-scaling on HTTP concurrency)
- Precursor project: ~/dev/richard/precursor
- /api/ml/inventory may timeout on Synapse (full-table scan, no partition pruning)
