# Description: Tracks current and upcoming tasks for HttpIngest.
# Description: Used by Claude Code to understand project state.

# HttpIngest TODO

## Current Sprint: ML Integration Phase A

### High Priority

- [ ] Implement `/api/ml/inventory` endpoint
  - Return available metrics, resources, and time ranges
  - Support filtering by datasource/resource type

- [ ] Implement `/api/ml/training-data` endpoint
  - Stream training data in format Precursor expects
  - Support profile parameter for metric filtering
  - Support start/end time range parameters

- [ ] Implement `/api/ml/profile-coverage` endpoint
  - Check which profile metrics are available
  - Return coverage percentage and gaps

### Medium Priority

- [ ] Implement `/api/ml/quality` endpoint
  - Data freshness metrics
  - Gap detection
  - Value range validation

- [ ] Add table partitioning for metric_data
  - Partition by timestamp for better query performance
  - Required for 56M+ rows in production

### Low Priority

- [ ] Update api-documentation.md with ML endpoints
- [ ] Add integration tests for ML endpoints
- [ ] Performance benchmarking with production data

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

- [ ] Streaming response support for large exports
- [ ] Webhook notifications for data quality issues
- [ ] Grafana dashboard templates
- [ ] Retention policy for old metric_data

## Notes

- HttpIngest does NOT directly integrate with quantum_mcp
- Precursor is the primary consumer of ML endpoints
- Local testing uses `collector` profile (ExecuteTime, ThreadCount, etc.)
- Production testing targets 56M+ rows of metric data
