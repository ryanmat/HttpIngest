# Description: Tracks current and upcoming tasks for HttpIngest.
# Description: Used by Claude Code to understand project state.

# HttpIngest TODO

## Current Sprint: Data Lake Only Mode (v32)

### Completed

- [x] Deploy v32 with Data Lake only mode (2026-01-26)
  - Disabled PostgreSQL hot cache (cost savings)
  - Scaled replicas from 30 to 1-5 range
  - Fixed "pool is closed" errors from token refresh bug
- [x] Fix Synapse Serverless SQL integration (2026-01-26)
  - Enabled Synapse for ML query layer
  - Fixed authentication (ActiveDirectoryMsi)
  - Fixed firewall (AllowAllWindowsAzureIps)
  - Fixed Parquet path wildcards (year=*/month=*/...)

### Architecture Change

The application now runs in **Data Lake only mode**:
- Primary storage: Azure Data Lake Gen2 (stlmingestdatalake)
- Hot cache: Disabled (PostgreSQL not needed)
- Synapse: Disabled (ML queries via Data Lake if needed later)
- Dashboards: To be handled in LogicMonitor directly

### Optional Future Work

- [ ] Re-enable hot cache if real-time dashboards needed (requires fixing token refresh bug)
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
- Current production version: v32 (Data Lake only mode)
- Mode: datalake_only (PostgreSQL disabled, Synapse enabled)
- Data Lake account: stlmingestdatalake
- Synapse server: syn-lm-analytics-ondemand.sql.azuresynapse.net
- Replicas: 1-5 (auto-scaling on HTTP concurrency)
- ML endpoints use Synapse Serverless SQL (~$5/TB scanned)
