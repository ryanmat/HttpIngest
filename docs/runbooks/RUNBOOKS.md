# Operations Runbooks - LogicMonitor Data Pipeline

Operational procedures for common tasks and incident response.

---

## Table of Contents

1. [Deployment](#deployment)
2. [Incident Response](#incident-response)
3. [Maintenance](#maintenance)
4. [Scaling](#scaling)
5. [Troubleshooting](#troubleshooting)
6. [Rollback](#rollback)

---

## Deployment

### Standard Deployment (v12)

**Pre-Deployment Checklist:**
- [ ] All tests passing
- [ ] Code reviewed and approved
- [ ] Environment variables configured
- [ ] Secrets synced to Key Vault
- [ ] Monitoring dashboards ready
- [ ] Alert recipients configured

**Deployment Steps:**

```bash
# 1. Verify authentication
az account show

# 2. Run deployment script
./scripts/deploy.sh v12 feature/production-redesign

# 3. Monitor deployment
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table

# 4. Verify health
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health

# 5. Check logs
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --follow
```

**Post-Deployment Verification:**

```bash
# Check all endpoints
./scripts/verify_deployment.sh

# Monitor for 15 minutes
watch -n 30 'curl -s https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health | jq'
```

**Expected Duration:** 10-15 minutes

---

## Incident Response

### High Error Rate (5xx Errors)

**Symptoms:**
- Alert: "High-5xx-Error-Rate" triggered
- Users reporting failures

**Investigation:**

```bash
# 1. Check current error rate
az monitor metrics list \
  --resource /subscriptions/{sub}/resourceGroups/CTA_Resource_Group/providers/microsoft.insights/components/httpdatapublisher \
  --metric "requests/failed" \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S)Z

# 2. Check recent logs for errors
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --tail 100 | grep ERROR

# 3. Check Application Insights
# Navigate to: https://portal.azure.com → Application Insights → Failures
```

**Common Causes:**

1. **Database Connection Issues**
   ```bash
   # Check PostgreSQL status
   az postgres flexible-server show \
     --name rm-postgres \
     --resource-group CTA_Resource_Group

   # Refresh Azure AD token
   TOKEN=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)

   # Update container app
   az containerapp update \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --set-env-vars "PGPASSWORD=$TOKEN"
   ```

2. **Redis Connection Issues**
   ```bash
   # Check Redis status
   az redis show \
     --name lm-data-pipeline-redis \
     --resource-group CTA_Resource_Group

   # Test connectivity
   az containerapp exec \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --command "redis-cli -h lm-data-pipeline-redis.redis.cache.windows.net -p 6380 --tls ping"
   ```

3. **Memory/CPU Exhaustion**
   ```bash
   # Check resource usage
   az monitor metrics list \
     --resource /subscriptions/{sub}/resourceGroups/CTA_Resource_Group/providers/Microsoft.App/containerApps/ca-cta-lm-ingest \
     --metric "WorkingSetBytes,UsageNanoCores" \
     --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z

   # Scale up if needed (see Scaling section)
   ```

**Resolution Steps:**

1. If database issue → Refresh token (see above)
2. If memory/CPU issue → Scale up resources
3. If Redis issue → Restart Redis or disable (set USE_REDIS=false)
4. If persistent → Rollback to previous version

---

### High Response Time

**Symptoms:**
- Alert: "High-Response-Time" triggered
- Users reporting slow responses

**Investigation:**

```bash
# Check response time metrics
az monitor metrics list \
  --resource /subscriptions/{sub}/resourceGroups/CTA_Resource_Group/providers/microsoft.insights/components/httpdatapublisher \
  --metric "requests/duration" \
  --aggregation Average \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z
```

**Common Causes:**

1. **Database Query Performance**
   - Check slow queries in PostgreSQL
   - Review query execution plans
   - Consider adding indexes

2. **High Load**
   ```bash
   # Check request rate
   az monitor metrics list \
     --resource /subscriptions/{sub}/resourceGroups/CTA_Resource_Group/providers/microsoft.insights/components/httpdatapublisher \
     --metric "requests/count" \
     --aggregation Count \
     --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z

   # Scale out if needed
   az containerapp update \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --min-replicas 5
   ```

---

### Service Unavailable

**Symptoms:**
- Health endpoint returns 503
- All requests failing

**Investigation:**

```bash
# 1. Check container status
az containerapp show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "{status:properties.runningStatus, health:properties.health}"

# 2. Check replica status
az containerapp replica list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group

# 3. Check recent revisions
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group
```

**Resolution:**

```bash
# Option 1: Restart app
az containerapp revision restart \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision <latest-revision-name>

# Option 2: Rollback (if recent deployment)
az containerapp revision activate \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision <previous-stable-revision>
```

---

## Maintenance

### Database Migrations

**Running Migrations:**

```bash
# 1. Connect to container
az containerapp exec \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --command "/bin/bash"

# 2. Check current migration version
uv run alembic current

# 3. Run migrations
uv run alembic upgrade head

# 4. Verify
uv run alembic history
```

**Creating New Migration:**

```bash
# 1. Make schema changes in models
# 2. Generate migration
uv run alembic revision --autogenerate -m "description"

# 3. Review migration file
# 4. Test in development
# 5. Deploy to production
```

### Token Refresh (Azure AD)

**Automatic via Deployment:**
```bash
./scripts/deploy.sh v12 feature/production-redesign
```

**Manual Refresh:**
```bash
# Get new token
TOKEN=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)

# Update container app
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars "PGPASSWORD=$TOKEN"
```

**Token expires in:** 90 minutes

### Log Retention

**View logs:**
```bash
# Last hour
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --tail 1000

# Follow live logs
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --follow
```

**Log retention:** 90 days in Application Insights

---

## Scaling

### Horizontal Scaling (Replicas)

**Scale Out (More Replicas):**
```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --min-replicas 5 \
  --max-replicas 30
```

**Scale In (Fewer Replicas):**
```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --min-replicas 2 \
  --max-replicas 20
```

**Current scaling:**
- Min: 2 replicas
- Max: 20 replicas
- Trigger: 100 concurrent requests

### Vertical Scaling (Resources)

**Increase CPU/Memory:**
```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --cpu 2.0 \
  --memory 4Gi
```

**Current resources:**
- CPU: 1.0 cores
- Memory: 2Gi
- Storage: 4Gi

### Database Scaling

**PostgreSQL:**
```bash
# Check current tier
az postgres flexible-server show \
  --name rm-postgres \
  --resource-group CTA_Resource_Group \
  --query "{tier:sku.tier, name:sku.name}"

# Scale up (requires restart)
az postgres flexible-server update \
  --name rm-postgres \
  --resource-group CTA_Resource_Group \
  --sku-name Standard_D4s_v3
```

**Redis:**
```bash
# Check current tier
az redis show \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --query "{sku:sku.name, capacity:sku.capacity}"

# Scale up
az redis update \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --sku Standard \
  --vm-size c1
```

---

## Troubleshooting

### Database Connection Failures

**Symptoms:**
- Logs show "connection refused" or "authentication failed"

**Diagnosis:**
```bash
# 1. Check PostgreSQL status
az postgres flexible-server show \
  --name rm-postgres \
  --resource-group CTA_Resource_Group

# 2. Check firewall rules
az postgres flexible-server firewall-rule list \
  --server-name rm-postgres \
  --resource-group CTA_Resource_Group

# 3. Test connection from container
az containerapp exec \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --command "pg_isready -h rm-postgres.postgres.database.azure.com -U ryan.matuszewski@logicmonitor.com"
```

**Fix:**
- Refresh Azure AD token (see Maintenance section)
- Verify firewall allows Container App IPs
- Check network connectivity

### Redis Connection Failures

**Symptoms:**
- Logs show "ECONNREFUSED" or "Redis connection failed"
- WebSocket/SSE features not working

**Diagnosis:**
```bash
# 1. Check Redis status
az redis show \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --query "{state:provisioningState, sslPort:sslPort}"

# 2. Test connectivity
az containerapp exec \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --command "redis-cli -h lm-data-pipeline-redis.redis.cache.windows.net -p 6380 --tls ping"
```

**Fix:**
- Verify REDIS_URL environment variable
- Check Redis firewall rules
- Temporary workaround: Set USE_REDIS=false (uses in-memory, single replica only)

### High Memory Usage

**Symptoms:**
- Alert: "High-Memory-Usage" triggered
- OOM kills in logs

**Diagnosis:**
```bash
# Check memory metrics
az monitor metrics list \
  --resource /subscriptions/{sub}/resourceGroups/CTA_Resource_Group/providers/Microsoft.App/containerApps/ca-cta-lm-ingest \
  --metric "WorkingSetBytes" \
  --aggregation Average,Maximum \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z
```

**Fix:**
1. Scale up memory (see Scaling section)
2. Check for memory leaks in logs
3. Review database connection pooling settings
4. Reduce MESSAGE_BUFFER_SIZE if needed

---

## Rollback

### Rollback to Previous Version

**List Recent Revisions:**
```bash
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table
```

**Activate Previous Revision:**
```bash
# Replace <revision-name> with actual revision (e.g., ca-cta-lm-ingest--v11)
az containerapp revision activate \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision <revision-name>
```

**Verify Rollback:**
```bash
# Check active revision
az containerapp show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "properties.latestRevisionName"

# Test health
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
```

**Rollback Time:** 2-5 minutes

---

## Emergency Contacts

**On-Call:** See PagerDuty rotation
**Azure Support:** https://portal.azure.com → Help + Support
**LogicMonitor Internal:** #cta-ops Slack channel

---

## Monitoring Links

- **Application Insights:** https://portal.azure.com → httpdatapublisher
- **Container App:** https://portal.azure.com → ca-cta-lm-ingest
- **Health Endpoint:** https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
- **Grafana:** https://grafana.logicmonitor.com (configure SimpleJSON datasource)
