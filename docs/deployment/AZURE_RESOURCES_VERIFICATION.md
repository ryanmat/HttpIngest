# Azure Resources Verification

## Current Azure Subscription Configuration

###  Confirmed Existing Resources

All resources are in the **same subscription**:

**Subscription ID:** `1eae27d8-cbaa-43fd-9f60-ce33de2c69b6`
**Resource Group:** `CTA_Resource_Group`
**Region:** East US
**Tenant ID:** `9e10c96a-3ffe-4f5e-9727-89500ac54a16`

---

### Existing Resources (All Confirmed)

#### 1. Azure Container App 
- **Name:** `ca-cta-lm-ingest`
- **Environment:** `cae-cta-lm-ingest`
- **FQDN:** `ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io`
- **System Assigned Identity:** `824aa5bb-0720-409a-b786-ef2ef5c33f5f`
- **Current Status:** Running
- **Current Version:** v10 (per CLAUDE.md)

#### 2. Azure Container Registry 
- **Name:** `acrctalmhttps001`
- **Server:** `acrctalmhttps001.azurecr.io`
- **Image Repository:** `lm-http-ingest`
- **Current Image:** `lm-http-ingest:latest`
- **Authentication:** Secret-based (stored in Container App)

#### 3. Azure PostgreSQL Flexible Server 
- **Host:** `rm-postgres.postgres.database.azure.com`
- **Database:** `postgres`
- **Version:** PostgreSQL 17.5
- **Authentication:** Azure AD (ryan.matuszewski@logicmonitor.com)
- **Token Expiry:** 90 minutes
- **Connection:** Configured in Container App environment variables

---

###  NEW Resource Required: Azure Cache for Redis

**Current Status:**  **NOT CONFIGURED** - Using placeholder

**Current Configuration:**
```yaml
REDIS_URL: redis://your-redis-host:6379  # ← PLACEHOLDER!
USE_REDIS: true
```

**Impact:**
- Real-time streaming (WebSocket/SSE) will use **in-memory fallback broker**
- This works for single-instance deployment
- Multi-instance scaling will need Redis for pub/sub across replicas

**Options:**

#### Option A: Provision Azure Cache for Redis (Recommended for Production)

**Pros:**
- Proper pub/sub across multiple container instances
- Supports horizontal scaling (2-20 replicas configured)
- Persistent message broker

**Cons:**
- Additional cost (~$15-30/month for Basic tier)
- Requires provisioning

**To Provision:**
```bash
az redis create \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --location eastus \
  --sku Basic \
  --vm-size c0 \
  --enable-non-ssl-port false

# Get connection details
REDIS_KEY=$(az redis list-keys \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --query primaryKey \
  --output tsv)

REDIS_HOST=$(az redis show \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --query hostName \
  --output tsv)

# Update Container App
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars "REDIS_URL=rediss://:${REDIS_KEY}@${REDIS_HOST}:6380"
```

#### Option B: Use In-Memory Fallback (Current Behavior)

**Pros:**
- No additional cost
- No provisioning required
- Works immediately

**Cons:**
- Only works with single container instance
- Messages not shared across replicas
- Limited to 1 replica (need to set `minReplicas: 1, maxReplicas: 1`)

**To Use In-Memory:**
```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars "USE_REDIS=false" \
  --min-replicas 1 \
  --max-replicas 1
```

---

## Updated Container App Configuration

### Current Environment Variables

####  Correct (Existing Resources)
```yaml
# Database
POSTGRES_HOST: rm-postgres.postgres.database.azure.com
POSTGRES_DB: postgres
POSTGRES_USER: ryan.matuszewski@logicmonitor.com
POSTGRES_PORT: 5432
USE_AZURE_AD_AUTH: true

# Legacy compatibility
PGHOST: rm-postgres.postgres.database.azure.com
PGDATABASE: postgres
PGUSER: ryan.matuszewski@logicmonitor.com

# Application
LOG_LEVEL: INFO
PYTHONUNBUFFERED: 1
FUNCTIONS_WORKER_RUNTIME: python
```

####  Needs Decision (Redis)
```yaml
# Option A: Use Azure Cache for Redis (after provisioning)
REDIS_URL: rediss://:XXXXX@lm-data-pipeline-redis.redis.cache.windows.net:6380
USE_REDIS: true

# Option B: Use in-memory (current behavior)
REDIS_URL: redis://localhost:6379  # ignored when USE_REDIS=false
USE_REDIS: false
```

####  New Configuration (Already Added)
```yaml
# Real-time Streaming
MAX_WEBSOCKET_CONNECTIONS: 500
RATE_LIMIT_MESSAGES_PER_SECOND: 50
RATE_LIMIT_BURST_SIZE: 100
MESSAGE_BUFFER_SIZE: 1000
CLIENT_STATE_RETENTION_HOURS: 24

# Background Tasks
DATA_PROCESSING_INTERVAL: 30
METRIC_PUBLISHING_INTERVAL: 10
HEALTH_MONITORING_INTERVAL: 60
```

---

## Resource Scaling Configuration

### Current Scaling (Updated)
```yaml
scale:
  minReplicas: 2  # ← Increased from 1
  maxReplicas: 20  # ← Increased from 10
  rules:
    - http:
        concurrentRequests: 100
```

###  Scaling Consideration

**If using in-memory Redis fallback (Option B):**
- **MUST set:** `minReplicas: 1, maxReplicas: 1`
- Reason: In-memory broker doesn't share state across replicas

**If using Azure Cache for Redis (Option A):**
- **Can use:** `minReplicas: 2, maxReplicas: 20` (as configured)
- Reason: Redis pub/sub properly distributes messages across replicas

---

## Resources Summary

###  Same Subscription - Confirmed
- **Subscription:** 1eae27d8-cbaa-43fd-9f60-ce33de2c69b6
- **Resource Group:** CTA_Resource_Group
- **Region:** East US

###  Existing Resources (No Changes Needed)
1. Container App: `ca-cta-lm-ingest`
2. Container Registry: `acrctalmhttps001`
3. PostgreSQL: `rm-postgres`
4. Managed Environment: `cae-cta-lm-ingest`

###  Decision Required
**Redis for Pub/Sub:** Choose Option A or B above

---

## Recommended Deployment Path

### Path 1: Deploy with In-Memory (Fastest)
1. Update Redis config to use in-memory:
   ```bash
   # Update container-app-config.yaml
   USE_REDIS: false
   ```
2. Set scaling to single instance:
   ```bash
   minReplicas: 1
   maxReplicas: 1
   ```
3. Deploy immediately with `./scripts/deploy.sh v12`
4. All features work except multi-instance real-time streaming

### Path 2: Provision Redis First (Recommended)
1. Provision Azure Cache for Redis (~5 minutes)
2. Update `container-app-config.yaml` with actual Redis connection string
3. Keep scaling configuration as-is (2-20 replicas)
4. Deploy with `./scripts/deploy.sh v12`
5. All features work including multi-instance streaming

---

## Pre-Deployment Checklist

- [x] All resources in same subscription: `1eae27d8-cbaa-43fd-9f60-ce33de2c69b6`
- [x] All resources in same resource group: `CTA_Resource_Group`
- [x] Container App configured: `ca-cta-lm-ingest`
- [x] Container Registry accessible: `acrctalmhttps001`
- [x] PostgreSQL configured: `rm-postgres`
- [x] Database migrations ready: `alembic/versions/`
- [ ] **DECISION NEEDED:** Redis configuration (Option A or B)
- [ ] Container app config updated with Redis decision
- [ ] Scaling configuration aligned with Redis choice

---

## Final Verification Command

Run this to verify current Azure resources:

```bash
# Verify subscription
az account show --query "{subscription:name, id:id}" -o table

# Verify resource group resources
az resource list \
  --resource-group CTA_Resource_Group \
  --output table

# Verify Container App status
az containerapp show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "{name:name, status:properties.runningStatus, replicas:properties.template.scale}" \
  -o json

# Verify PostgreSQL
az postgres flexible-server show \
  --name rm-postgres \
  --resource-group CTA_Resource_Group \
  --query "{name:name, state:state, version:version}" \
  -o table
```

---

## Conclusion

 **All existing resources are in the same Azure subscription**
 **No changes to existing resources needed**
 **Decision needed on Redis configuration before deployment**

**Recommendation:**
- For **immediate testing**: Use in-memory (Path 1)
- For **production use**: Provision Redis (Path 2)
