# Description: Tracks current and upcoming tasks for HttpIngest.
# Description: Used by Claude Code to understand project state.

# HttpIngest TODO

## Current Sprint: ML Integration Phase B

### High Priority

- [x] Implement `/api/ml/quality` endpoint (v21)
  - Data freshness metrics
  - Gap detection
  - Value range validation

- [x] Add table partitioning for metric_data (DEPLOYED)
  - Partitioned by month (metric_data_2026_01, etc.)
  - 441K rows migrated successfully
  - Materialized views recreated

### Medium Priority

- [x] Update api-documentation.md with ML endpoints (v21)
- [x] Add integration tests for ML endpoints with real database
  - 32 async integration tests covering inventory, training-data, profile-coverage, quality
  - Performance tests verify sub-5s response times
  - Fixed SQL column references in ml_service.py (md.datasource_id joins)
- [x] Performance benchmarking with production data
  - 488K rows across 3 partitions
  - get_inventory: ~2.4s (1,336 metrics, 153 resources)
  - get_training_data(limit=1000): ~1.1s
  - get_training_data(profile filter): ~450ms
  - get_profile_coverage: ~270ms (100% on all profiles)
  - get_data_quality(24h): ~2.5s

### Low Priority

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

- [ ] Streaming response support for large exports
- [ ] Webhook notifications for data quality issues
- [ ] Grafana dashboard templates
- [ ] Retention policy for old metric_data

## Notes

- HttpIngest does NOT directly integrate with quantum_mcp
- Precursor is the primary consumer of ML endpoints
- Profiles support both LM metric names and Precursor standard names
- Current profile coverage (v21 with partitioned data):
  - collector: 100% (38/38 features)
  - kubernetes: 100% (58/58 features)
  - cloud_compute: 100% (46/46 features)
  - network: 100% (21/21 features)
  - database: 100% (10/10 features)
  - application: 100% (25/25 features)
