# Load Testing Guide - LogicMonitor Data Pipeline

Comprehensive load testing procedures to verify system stability and performance under stress.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Load Testing Tools](#load-testing-tools)
3. [Test Scenarios](#test-scenarios)
4. [Running Tests](#running-tests)
5. [Performance Targets](#performance-targets)
6. [Analyzing Results](#analyzing-results)

---

## Prerequisites

### Install Load Testing Tools

```bash
# Install Locust (Python-based load testing)
pip install locust

# Or using uv
uv add --dev locust

# Install additional dependencies
pip install websocket-client
```

### Environment Setup

```bash
# Set target environment
export LOAD_TEST_TARGET="https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io"

# Optional: API key if required
export API_KEY="your-api-key"
```

---

## Load Testing Tools

### Locust (Recommended)

Python-based load testing with real-time web UI.

**Features:**
- Real-time metrics dashboard
- Distributed load generation
- Python-based test scenarios
- Export results to CSV

### Alternative: k6

JavaScript-based load testing tool.

```bash
# Install k6
brew install k6  # macOS
# or
curl https://github.com/grafana/k6/releases/download/v0.47.0/k6-v0.47.0-linux-amd64.tar.gz -L | tar xvz --strip-components 1
```

---

## Test Scenarios

### 1. Baseline Test

**Objective:** Establish performance baseline with normal load.

**Configuration:**
- Users: 10
- Spawn rate: 1 user/second
- Duration: 5 minutes
- Target RPS: 50

**Run:**
```bash
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --users 10 \
  --spawn-rate 1 \
  --run-time 5m \
  --headless \
  --only-summary
```

**Expected Results:**
- Median response time: < 200ms
- 95th percentile: < 500ms
- Error rate: < 0.1%
- Throughput: 50+ RPS

### 2. Load Test

**Objective:** Test performance under expected production load.

**Configuration:**
- Users: 100
- Spawn rate: 10 users/second
- Duration: 15 minutes
- Target RPS: 500

**Run:**
```bash
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --users 100 \
  --spawn-rate 10 \
  --run-time 15m \
  --headless \
  --csv results/load-test
```

**Expected Results:**
- Median response time: < 300ms
- 95th percentile: < 1000ms
- Error rate: < 1%
- Throughput: 500+ RPS

### 3. Stress Test

**Objective:** Find breaking point and test failover behavior.

**Configuration:**
- Users: Start at 100, increase to 500
- Spawn rate: 20 users/second
- Duration: 20 minutes
- Target RPS: 2000+

**Run:**
```bash
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --users 500 \
  --spawn-rate 20 \
  --run-time 20m \
  --headless \
  --csv results/stress-test
```

**Monitor:**
- CPU usage → should trigger autoscaling
- Memory usage → should stay below 80%
- Error rate → should remain acceptable
- Response time → may degrade but should stabilize

### 4. Spike Test

**Objective:** Test behavior under sudden traffic spikes.

**Configuration:**
- Users: 500
- Spawn rate: 100 users/second (rapid spike)
- Duration: 5 minutes
- Pattern: Spike, hold, drop

**Run:**
```bash
# Use SpikeTestUser class
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --user-class SpikeTestUser \
  --users 500 \
  --spawn-rate 100 \
  --run-time 5m \
  --headless
```

**Expected Behavior:**
- Auto-scaling should trigger quickly
- Some requests may be queued/delayed
- No cascading failures
- System recovers after spike

### 5. Endurance Test (Soak Test)

**Objective:** Verify stability over extended period.

**Configuration:**
- Users: 50
- Spawn rate: 5 users/second
- Duration: 4-8 hours
- Target RPS: 200

**Run:**
```bash
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --users 50 \
  --spawn-rate 5 \
  --run-time 4h \
  --headless \
  --csv results/endurance-test
```

**Monitor For:**
- Memory leaks (increasing memory over time)
- Performance degradation
- Connection pool exhaustion
- Token expiration (Azure AD tokens expire after 90 min)

---

## Running Tests

### Interactive Mode (with Web UI)

```bash
# Start Locust with web UI
locust -f tests/load/locustfile.py --host $LOAD_TEST_TARGET

# Open browser to http://localhost:8089
# Configure users and spawn rate in UI
# Start test and monitor real-time metrics
```

### Headless Mode (CI/CD)

```bash
# Run test without UI
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --users 100 \
  --spawn-rate 10 \
  --run-time 10m \
  --headless \
  --csv results/ci-load-test \
  --html results/ci-load-test-report.html
```

### Distributed Load Generation

For high load tests, distribute across multiple machines:

**Master:**
```bash
locust -f tests/load/locustfile.py \
  --host $LOAD_TEST_TARGET \
  --master \
  --expect-workers 3
```

**Workers (run on 3 separate machines):**
```bash
locust -f tests/load/locustfile.py \
  --worker \
  --master-host <master-ip>
```

---

## Performance Targets

### Response Times

| Endpoint | Median | 95th Percentile | 99th Percentile |
|----------|--------|-----------------|-----------------|
| /api/HttpIngest | < 200ms | < 500ms | < 1000ms |
| /metrics/prometheus | < 300ms | < 1000ms | < 2000ms |
| /grafana/query | < 500ms | < 1500ms | < 3000ms |
| /api/health | < 50ms | < 100ms | < 200ms |
| /export/* | < 1000ms | < 3000ms | < 5000ms |

### Throughput

| Load Level | Target RPS | Expected Replicas |
|------------|-----------|-------------------|
| Light | 50 | 2 |
| Normal | 200-500 | 2-5 |
| High | 1000-2000 | 5-15 |
| Peak | 3000+ | 15-20 |

### Resource Usage

| Resource | Normal Load | High Load | Critical Threshold |
|----------|-------------|-----------|-------------------|
| CPU | < 40% | < 70% | 80% |
| Memory | < 60% | < 75% | 85% |
| Database Connections | < 50 | < 150 | 200 |
| Redis Connections | < 30 | < 80 | 100 |

### Error Rates

| Scenario | Acceptable Error Rate |
|----------|----------------------|
| Normal Load | < 0.1% |
| High Load | < 1% |
| Stress Test | < 5% |
| Spike Test | < 10% (temporary) |

---

## Analyzing Results

### Locust Reports

After test completes, review:

1. **CSV Files** (in `results/` directory):
   - `*_stats.csv` - Request statistics
   - `*_failures.csv` - Failure details
   - `*_exceptions.csv` - Exception log

2. **HTML Report:**
   - Open `results/*-report.html` in browser
   - Review charts and statistics

### Key Metrics to Analyze

1. **Response Time Distribution:**
   - Check if 95th percentile meets targets
   - Look for outliers in 99th percentile

2. **Failure Rate:**
   - Acceptable: < 1% under normal load
   - Investigate all 5xx errors

3. **Throughput:**
   - Actual RPS vs. target RPS
   - Should meet or exceed targets

4. **Auto-scaling Behavior:**
   ```bash
   # Check replica count during test
   az containerapp revision list \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --output table
   ```

5. **Resource Utilization:**
   ```bash
   # Check CPU/Memory metrics
   az monitor metrics list \
     --resource /subscriptions/{sub}/resourceGroups/CTA_Resource_Group/providers/Microsoft.App/containerApps/ca-cta-lm-ingest \
     --metric "WorkingSetBytes,UsageNanoCores" \
     --start-time "2025-01-14T12:00:00Z" \
     --end-time "2025-01-14T13:00:00Z"
   ```

### Application Insights Analysis

View in Azure Portal:

1. **Performance** → Review slow requests
2. **Failures** → Investigate errors
3. **Metrics** → Check custom metrics:
   - Request rate
   - Response time
   - Dependency calls
   - Exception rate

4. **Live Metrics** → Real-time monitoring during test

---

## Troubleshooting Load Tests

### Issue: High Error Rate

**Possible Causes:**
1. Database connection pool exhausted
2. Rate limiting triggered
3. Memory exhaustion
4. Azure AD token expiration

**Solutions:**
- Increase DB pool size: `DB_POOL_SIZE=50`
- Disable rate limiting for test
- Increase memory allocation
- Refresh token mid-test

### Issue: Slow Response Times

**Possible Causes:**
1. Database query performance
2. Insufficient replicas
3. Network latency
4. Inefficient queries

**Solutions:**
- Add database indexes
- Pre-scale replicas before test
- Run test from same Azure region
- Review slow query logs

### Issue: Auto-scaling Not Triggering

**Possible Causes:**
1. Scaling rules not configured
2. Load not reaching threshold
3. Cool-down period active

**Solutions:**
- Verify scaling rules:
  ```bash
  az containerapp show \
    --name ca-cta-lm-ingest \
    --resource-group CTA_Resource_Group \
    --query "properties.template.scale"
  ```
- Increase load intensity
- Wait for cool-down period (5 minutes)

---

## Pre-Production Load Test Checklist

- [ ] All tests passing in CI/CD
- [ ] Database migrations applied
- [ ] Secrets configured in Key Vault
- [ ] Monitoring dashboards created
- [ ] Alerts configured and tested
- [ ] Baseline test completed successfully
- [ ] Load test (100 users) completed successfully
- [ ] Stress test showed graceful degradation
- [ ] Spike test showed recovery
- [ ] Endurance test (2+ hours) completed without issues
- [ ] Auto-scaling behavior verified
- [ ] Resource utilization within targets
- [ ] Error rate within acceptable limits
- [ ] Token refresh tested
- [ ] Rollback procedure tested
- [ ] Runbooks reviewed and validated

---

## Load Test Schedule

### Pre-Deployment
1. Baseline test → Establish baseline metrics
2. Load test → Verify production capacity
3. Stress test → Find breaking point
4. Spike test → Verify auto-scaling

### Post-Deployment
1. Smoke test (10 users, 5 min) → Verify deployment
2. Load test (100 users, 15 min) → Confirm performance
3. Monitor for 1 hour → Watch for issues

### Regular Testing
- **Weekly:** Baseline test
- **Monthly:** Full load test suite
- **Quarterly:** Endurance test (8 hours)

---

## Reporting Results

### Test Report Template

```
# Load Test Report

**Date:** YYYY-MM-DD
**Version:** v12
**Test Type:** Load Test
**Duration:** 15 minutes

## Configuration
- Users: 100
- Spawn Rate: 10/sec
- Target: https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io

## Results
- Total Requests: 45,000
- Failures: 0.05%
- Median Response Time: 185ms
- 95th Percentile: 420ms
- Throughput: 50 RPS

## Resource Utilization
- CPU: 45% average, 65% peak
- Memory: 60% average, 72% peak
- Replicas: Auto-scaled from 2 to 6

## Conclusions
✅ All performance targets met
✅ Auto-scaling functioned correctly
✅ Error rate within acceptable limits
✅ Ready for production deployment

## Issues Found
None

## Recommendations
- Consider increasing min replicas to 3 for faster warm-up
```

---

## Next Steps

After successful load testing:
1. Review and approve test results
2. Update performance benchmarks
3. Document any configuration changes
4. Proceed with production deployment
