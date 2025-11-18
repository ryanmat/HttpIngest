# Production Deployment Runbook - v12

**Date:** 2025-01-15
**Version:** v12.0.0
**Branch:** feature/production-redesign
**Estimated Time:** 30 minutes

---

## Pre-Deployment Checklist

- [x] All code committed to feature/production-redesign
- [x] LogicMonitor integration tested and verified
- [x] End-to-end data flow validated
- [ ] Secrets uploaded to Key Vault
- [ ] Monitoring setup complete
- [ ] Alert recipients configured

---

## Step 1: Upload Secrets to Key Vault (5 minutes)

**Purpose:** Store sensitive credentials securely in Azure Key Vault

### Get PostgreSQL Password (Azure AD Token)

```bash
# Get fresh token (valid for 90 minutes)
POSTGRES_TOKEN=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)

echo "Token obtained (expires in 90 min)"
```

### Upload Secrets

```bash
# PostgreSQL password (Azure AD token)
az keyvault secret set \
  --vault-name rm-cta-keyvault \
  --name postgres-password \
  --value "$POSTGRES_TOKEN"

# Redis password (get from Azure Portal if needed)
az redis list-keys \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --query primaryKey -o tsv

REDIS_KEY=$(az redis list-keys --name lm-data-pipeline-redis --resource-group CTA_Resource_Group --query primaryKey -o tsv)

az keyvault secret set \
  --vault-name rm-cta-keyvault \
  --name redis-password \
  --value "$REDIS_KEY"

# Application Insights connection string
APP_INSIGHTS_CONN=$(az monitor app-insights component show \
  --app httpdatapublisher \
  --resource-group CTA_Resource_Group \
  --query connectionString -o tsv)

az keyvault secret set \
  --vault-name rm-cta-keyvault \
  --name app-insights-connection-string \
  --value "$APP_INSIGHTS_CONN"
```

### Verify Secrets

```bash
az keyvault secret list \
  --vault-name rm-cta-keyvault \
  --query "[].name" -o table
```

**Expected output:**
```
Result
------------------------
postgres-password
redis-password
app-insights-connection-string
```

**Checkpoint:**  All secrets uploaded

---

## Step 2: Run Monitoring Setup (5 minutes)

**Purpose:** Create Application Insights dashboards and alerts

```bash
# Make script executable
chmod +x scripts/setup_monitoring.sh

# Run setup
./scripts/setup_monitoring.sh
```

**Expected output:**
```
 Application Insights exists
 Action group created: lm-pipeline-alerts
 Alert created: High-5xx-Error-Rate
 Alert created: High-Response-Time
 Alert created: Low-Availability
 Alert created: High-CPU-Usage
 Alert created: High-Memory-Usage
 Alert created: High-Replica-Restart-Rate
```

### Configure Alert Recipients

```bash
# Add your email for alerts
az monitor action-group update \
  --name lm-pipeline-alerts \
  --resource-group CTA_Resource_Group \
  --add-action email ryan-alerts ryan.matuszewski@logicmonitor.com
```

**Checkpoint:**  Monitoring configured

---

## Step 3: Deploy v12 to Azure (10 minutes)

**Purpose:** Deploy new version with LogicMonitor fixes

### Check Current Version

```bash
az containerapp show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "properties.latestRevisionName" -o tsv
```

**Current:** `ca-cta-lm-ingest--<revision>` (v10)

### Get Fresh Azure AD Token for Deployment

```bash
# Get new token (deployment needs fresh token)
export PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)

echo "Deployment token ready"
```

### Build and Deploy

```bash
# Build new container image
az acr build \
  --registry ctacontainerregistry \
  --image lm-data-pipeline:v12 \
  --file Dockerfile \
  .
```

**Expected:** Build completes successfully (~5 minutes)

```bash
# Update Container App with new image
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --image ctacontainerregistry.azurecr.io/lm-data-pipeline:v12 \
  --set-env-vars \
    "POSTGRES_HOST=rm-postgres.postgres.database.azure.com" \
    "POSTGRES_DB=postgres" \
    "POSTGRES_USER=ryan.matuszewski@logicmonitor.com" \
    "POSTGRES_PASSWORD=$PGPASSWORD" \
    "POSTGRES_PORT=5432" \
    "REDIS_URL=rediss://lm-data-pipeline-redis.redis.cache.windows.net:6380" \
    "USE_REDIS=false" \
    "APPLICATIONINSIGHTS_CONNECTION_STRING=$APP_INSIGHTS_CONN"
```

**Expected:** Deployment completes (~3 minutes)

### Monitor Deployment

```bash
# Watch revision deployment
watch -n 5 'az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "[].{Name:name, Active:properties.active, Health:properties.healthState, Replicas:properties.replicas}" \
  -o table'
```

**Wait for:**
- New revision appears
- Health: Healthy
- Replicas: 2+ running

**Checkpoint:**  v12 deployed

---

## Step 4: Verify Deployment Health (5 minutes)

**Purpose:** Ensure v12 is running correctly before enabling data ingestion

### Test Health Endpoint

```bash
curl -s https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health | jq
```

**Expected output:**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T...",
  "version": "1.0.0",
  "components": {
    "database": {
      "status": "healthy",
      "metric_count": 3
    },
    "streaming": {
      "status": "healthy",
      "active_websockets": 0
    },
    "background_tasks": {
      "data_processor": "running",
      "metric_publisher": "running",
      "health_monitor": "running"
    }
  }
}
```

**Key checks:**
-  `status: "healthy"`
-  `database.status: "healthy"`
-  `background_tasks.data_processor: "running"`

### Test Ingestion Endpoint

```bash
# Send test OTLP payload
curl -X POST \
  https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d '{
    "resourceMetrics": [{
      "resource": {
        "attributes": [
          {"key": "test", "value": {"stringValue": "deployment-test"}}
        ]
      },
      "scopeMetrics": [{
        "scope": {"name": "test-datasource"},
        "metrics": [{
          "name": "test.metric",
          "gauge": {
            "dataPoints": [{
              "timeUnixNano": "1715263558360000000",
              "asDouble": 1.0
            }]
          }
        }]
      }]
    }]
  }'
```

**Expected output:**
```json
{
  "id": 354,
  "status": "accepted"
}
```

**Key check:**
-  Returns 202 Accepted
-  Returns ID number

### Verify Data Processing (Wait 30 seconds)

```bash
# Wait for background processor
sleep 35

# Check database
PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv) \
psql \
  "host=rm-postgres.postgres.database.azure.com port=5432 dbname=postgres user=ryan.matuszewski@logicmonitor.com sslmode=require" \
  -c "SELECT COUNT(*) FROM metric_data;"
```

**Expected:**
```
 count
-------
     4
(1 row)
```

**Key check:**
-  Count increased from 3 to 4 (test metric processed)

### Check Application Logs

```bash
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --tail 50
```

**Look for:**
-  " Data processor started"
-  " Processed 1 records: ... 1 data points"
-  No ERROR messages

**Checkpoint:**  v12 healthy and processing data

---

## Step 5: 🚨 ENABLE LOGICMONITOR COLLECTOR PUBLISHER 🚨

**⏰ DO THIS NOW - After Step 4 verification passes**

### Update LogicMonitor Collector Configuration

1. **Log into LogicMonitor Portal**
2. **Navigate to:** Settings → Collectors → [Your Collector]
3. **Edit Collector Properties**
4. **Change:**
   ```properties
   # CHANGE THIS:
   enable.collector.publisher=false

   # TO THIS:
   enable.collector.publisher=true
   ```
5. **Save changes**
6. **Restart collector** (if required)

### Verify Configuration

After enabling, verify the collector settings show:

```properties
 enable.collector.publisher=true
 agent.publisher.name=http
 publisher.http.url=https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest
 agent.publisher.enable.auth=false
 publisher.dequeue.count=1
 collector.publisher.device.props=true
```

**Checkpoint:**  Collector Publisher ENABLED

---

## Step 6: Monitor Data Flow (60 minutes)

**Purpose:** Verify LogicMonitor data is flowing correctly

### Immediate Checks (First 5 minutes)

#### Watch for incoming data

```bash
# Monitor lm_metrics table for new inserts
watch -n 10 'PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv) \
  psql "host=rm-postgres.postgres.database.azure.com port=5432 dbname=postgres user=ryan.matuszewski@logicmonitor.com sslmode=require" \
  -c "SELECT COUNT(*) as total, MAX(ingested_at) as latest FROM lm_metrics;"'
```

**Expected:**
- Count increases every few seconds
- Latest timestamp updates

#### Check Application Insights Live Metrics

1. Open Azure Portal → Application Insights → httpdatapublisher
2. Click "Live Metrics"
3. Watch for:
   -  Incoming requests to /api/HttpIngest
   -  Request rate increasing
   -  Response time < 200ms
   -  Success rate 100%

#### Monitor Container Logs

```bash
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --follow
```

**Look for:**
```
 Processing OTLP data from LogicMonitor
 Inserted metric batch [ID]
 Processed N records: X resources, Y datasources, Z data points
```

**Red flags (should NOT see):**
```
 Database error
 Parsing failed
 ERROR in data processor
```

### 15-Minute Check

```bash
# Check processing stats
PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv) \
psql "host=rm-postgres.postgres.database.azure.com port=5432 dbname=postgres user=ryan.matuszewski@logicmonitor.com sslmode=require" \
-c "
SELECT
  (SELECT COUNT(*) FROM lm_metrics) as raw_records,
  (SELECT COUNT(*) FROM processing_status WHERE status='success') as processed,
  (SELECT COUNT(*) FROM processing_status WHERE status='failed') as failed,
  (SELECT COUNT(*) FROM resources) as resources,
  (SELECT COUNT(*) FROM datasources) as datasources,
  (SELECT COUNT(*) FROM metric_definitions) as metrics,
  (SELECT COUNT(*) FROM metric_data) as datapoints;
"
```

**Expected:**
```
 raw_records | processed | failed | resources | datasources | metrics | datapoints
-------------+-----------+--------+-----------+-------------+---------+------------
         150 |       150 |      0 |        25 |          50 |     300 |       1500
```

**Key checks:**
-  processed ≈ raw_records (within 30 seconds)
-  failed = 0 (or very low)
-  datapoints growing steadily

### 30-Minute Check

```bash
# Check for top metrics
PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv) \
psql "host=rm-postgres.postgres.database.azure.com port=5432 dbname=postgres user=ryan.matuszewski@logicmonitor.com sslmode=require" \
-c "
SELECT
  ds.name as datasource,
  md.name as metric,
  md.metric_type,
  COUNT(*) as datapoint_count
FROM metric_data m
JOIN metric_definitions md ON m.metric_definition_id = md.id
JOIN datasources ds ON md.datasource_id = ds.id
GROUP BY ds.name, md.name, md.metric_type
ORDER BY datapoint_count DESC
LIMIT 10;
"
```

**Expected:**
```
           datasource            |      metric       | metric_type | datapoint_count
---------------------------------+-------------------+-------------+-----------------
 LogicMonitor_Collector_...      | CpuUsage          | sum         |             45
 LogicMonitor_Collector_...      | ProcessorCount    | gauge       |             45
 ...
```

### 60-Minute Check

#### Application Insights Metrics

1. Open Azure Portal → Application Insights → httpdatapublisher
2. Go to "Metrics"
3. Check:
   - **Request rate:** Should show steady traffic
   - **Response time:** Average < 200ms, P95 < 500ms
   - **Failed requests:** Should be 0 or near 0
   - **Server errors:** Should be 0

#### Check for Alerts

```bash
# Check if any alerts fired
az monitor metrics alert list \
  --resource-group CTA_Resource_Group \
  --query "[].{Name:name, Enabled:enabled}" -o table
```

**Expected:** No active alerts (all should be quiet)

**Checkpoint:**  Data flowing smoothly for 1 hour

---

## Step 7: Run Load Tests (15 minutes)

**Purpose:** Verify system performance under load

### Baseline Test

```bash
./scripts/run_load_test.sh baseline
```

**Expected results:**
```
Total Requests: ~3,000
Failures: < 1%
Median Response Time: < 200ms
95th Percentile: < 500ms
Requests/sec: 50+
```

### Review Results

```bash
# Open HTML report
open results/load-tests/baseline_*.html
```

**Checkpoint:**  Load test passed

---

## Post-Deployment Verification

### Final Database Check

```bash
PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv) \
psql "host=rm-postgres.postgres.database.azure.com port=5432 dbname=postgres user=ryan.matuszewski@logicmonitor.com sslmode=require" \
-c "
SELECT
  'lm_metrics' as table_name, COUNT(*) as count FROM lm_metrics
UNION ALL
SELECT 'resources', COUNT(*) FROM resources
UNION ALL
SELECT 'datasources', COUNT(*) FROM datasources
UNION ALL
SELECT 'metric_definitions', COUNT(*) FROM metric_definitions
UNION ALL
SELECT 'metric_data', COUNT(*) FROM metric_data
UNION ALL
SELECT 'processing_status (success)', COUNT(*) FROM processing_status WHERE status='success'
UNION ALL
SELECT 'processing_status (failed)', COUNT(*) FROM processing_status WHERE status='failed';
"
```

### Update Production Readiness Score

```
Before: 69/70 (98.5%)
After:  70/70 (100%) 

All tasks complete:
 Environment configurations
 Secret management
 Monitoring dashboards
 Alerts configured
 API documentation
 Operational runbooks
 Load testing complete
```

---

## Rollback Procedure (If Needed)

**Only if critical issues occur:**

```bash
# List revisions
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table

# Activate previous revision (v10)
az containerapp revision activate \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision <previous-revision-name>

# Disable collector publisher
# In LogicMonitor Portal: enable.collector.publisher=false
```

---

## Success Criteria

-  v12 deployed successfully
-  Health endpoint returns healthy
-  LogicMonitor data flowing into lm_metrics
-  Background processor normalizing data
-  No errors in Application Insights
-  No alerts triggered
-  Load test passed
-  Data queryable via export endpoints

---

## Next Steps After Deployment

1. **Create Grafana dashboards** (optional)
2. **Set up PowerBI reports** (optional)
3. **Configure webhook notifications** (optional)
4. **Implement data retention policies** (future)
5. **Add authentication to ingestion endpoint** (future)

---

**Deployment completed successfully!** 🎉

**Deployed by:** Ryan Matuszewski
**Version:** v12.0.0
**Date:** 2025-01-15
