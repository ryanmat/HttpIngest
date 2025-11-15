# Azure Deployment Guide

This guide explains how to deploy the LogicMonitor Data Pipeline to Azure Container Apps.

## Architecture

```
LogicMonitor → Azure Container App → Azure PostgreSQL 17
                     ↓
                Azure Cache for Redis
```

### Components

- **Azure Container App**: Hosts the data pipeline application
- **Azure PostgreSQL 17**: Stores normalized metrics and time-series data
- **Azure Cache for Redis**: Pub/sub messaging for real-time streaming
- **Azure Container Registry (ACR)**: Stores Docker images
- **Azure Active Directory**: Authentication for PostgreSQL

## Prerequisites

1. **Azure CLI** (v2.50+):
   ```bash
   az --version
   ```

2. **Azure Subscription** with access to:
   - Azure Container Apps
   - Azure PostgreSQL Flexible Server
   - Azure Cache for Redis
   - Azure Container Registry

3. **Resource Group**: `CTA_Resource_Group`

4. **Permissions**:
   - Contributor on Resource Group
   - Azure AD authentication for PostgreSQL

## Current Production Setup

### Container App

- **Name**: `ca-cta-lm-ingest`
- **Environment**: `cae-cta-lm-ingest`
- **Region**: East US
- **URL**: https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io

### Container Registry

- **Registry**: `acrctalmhttps001.azurecr.io`
- **Image**: `lm-http-ingest:latest` (or versioned tags like `v11`)

### Database

- **Server**: `rm-postgres.postgres.database.azure.com`
- **Database**: `postgres`
- **User**: `ryan.matuszewski@logicmonitor.com`
- **Auth**: Azure AD (token expires every 90 minutes)

### Redis

- **Host**: TBD - Configure in `REDIS_URL` environment variable
- **Port**: 6379 (default)
- **SSL**: Recommended for production

## Deployment Process

### Quick Deploy

Use the automated deployment script:

```bash
# Deploy latest from feature branch
./scripts/deploy.sh v12 feature/production-redesign

# Deploy from main
./scripts/deploy.sh v12 main
```

The script will:
1. Build Docker image in ACR from GitHub
2. Get fresh Azure AD token
3. Update Container App with new image
4. Run comprehensive health checks

### Manual Deployment

#### Step 1: Build Docker Image

Build from local code:

```bash
az acr build \
  --registry acrctalmhttps001 \
  --resource-group CTA_Resource_Group \
  --image lm-http-ingest:v12 \
  --file Dockerfile \
  .
```

Or build from GitHub:

```bash
az acr build \
  --registry acrctalmhttps001 \
  --resource-group CTA_Resource_Group \
  --image lm-http-ingest:v12 \
  --file Dockerfile \
  https://github.com/ryanmat/HttpIngest.git#feature/production-redesign
```

#### Step 2: Get Azure AD Token

```bash
TOKEN=$(az account get-access-token \
  --resource-type oss-rdbms \
  --query accessToken \
  --output tsv)

echo "Token acquired (expires in 90 minutes)"
```

#### Step 3: Update Container App

```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --image acrctalmhttps001.azurecr.io/lm-http-ingest:v12 \
  --set-env-vars "PGPASSWORD=$TOKEN" \
  --revision-suffix v12
```

#### Step 4: Verify Deployment

```bash
# Check revision status
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table

# Check health
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
```

## Environment Configuration

### Required Environment Variables

```yaml
# Database (Azure PostgreSQL with AD Auth)
USE_AZURE_AD_AUTH: true
POSTGRES_HOST: rm-postgres.postgres.database.azure.com
POSTGRES_DB: postgres
POSTGRES_USER: ryan.matuszewski@logicmonitor.com
POSTGRES_PORT: 5432
PGPASSWORD: <Azure AD token - auto-injected by deploy script>

# Redis (Azure Cache for Redis)
REDIS_URL: redis://your-redis-host:6379
USE_REDIS: true

# Application
LOG_LEVEL: INFO
PYTHONUNBUFFERED: 1
FUNCTIONS_WORKER_RUNTIME: python
```

### Optional Environment Variables

```yaml
# Real-time Streaming Limits
MAX_WEBSOCKET_CONNECTIONS: 500
RATE_LIMIT_MESSAGES_PER_SECOND: 50
RATE_LIMIT_BURST_SIZE: 100
MESSAGE_BUFFER_SIZE: 1000
CLIENT_STATE_RETENTION_HOURS: 24

# Background Task Intervals (seconds)
DATA_PROCESSING_INTERVAL: 30
METRIC_PUBLISHING_INTERVAL: 10
HEALTH_MONITORING_INTERVAL: 60
```

### Setting Environment Variables

```bash
# Set individual variable
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars "REDIS_URL=redis://your-redis-host:6379"

# Set multiple variables
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars \
    "MAX_WEBSOCKET_CONNECTIONS=1000" \
    "RATE_LIMIT_MESSAGES_PER_SECOND=100"
```

## Resource Configuration

### Current Resources

```yaml
resources:
  cpu: 1.0
  memory: 2Gi
  ephemeralStorage: 4Gi
```

### Scaling Configuration

```yaml
scale:
  minReplicas: 2
  maxReplicas: 20
  cooldownPeriod: 300
  rules:
    - http:
        concurrentRequests: 100
```

### Updating Resources

```bash
# Increase CPU and memory
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --cpu 2.0 \
  --memory 4Gi

# Update scaling limits
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --min-replicas 3 \
  --max-replicas 30
```

## Health Checks & Monitoring

### Health Probes

Configured in `container-app-config.yaml`:

```yaml
probes:
  # Liveness probe - restarts if unhealthy
  - httpGet:
      path: /api/health
      port: 8000
    initialDelaySeconds: 10
    periodSeconds: 30
    type: Liveness

  # Readiness probe - removes from load balancer if unhealthy
  - httpGet:
      path: /api/health
      port: 8000
    initialDelaySeconds: 5
    periodSeconds: 10
    type: Readiness
```

### Health Endpoints

- **FastAPI Health**: https://{fqdn}/api/health
- **Metrics Summary**: https://{fqdn}/api/metrics/summary
- **Prometheus Metrics**: https://{fqdn}/metrics/prometheus

### Monitoring Commands

```bash
# View logs
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --follow

# View recent logs
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --tail 100

# Check replica status
az containerapp replica list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table

# View metrics
az monitor metrics list \
  --resource /subscriptions/.../ca-cta-lm-ingest \
  --metric Requests \
  --start-time 2025-01-01T00:00:00Z
```

## Database Management

### Running Migrations

```bash
# Connect to a running replica
az containerapp exec \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --command "uv run alembic upgrade head"
```

### Database Access

```bash
# Get fresh token
TOKEN=$(az account get-access-token \
  --resource-type oss-rdbms \
  --query accessToken \
  --output tsv)

# Connect to PostgreSQL
psql "host=rm-postgres.postgres.database.azure.com \
      dbname=postgres \
      user=ryan.matuszewski@logicmonitor.com \
      password=$TOKEN \
      sslmode=require"
```

## Redis Setup (Azure Cache for Redis)

### Create Redis Instance

```bash
az redis create \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --location eastus \
  --sku Basic \
  --vm-size c0 \
  --enable-non-ssl-port false
```

### Get Redis Connection String

```bash
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

echo "REDIS_URL=rediss://:${REDIS_KEY}@${REDIS_HOST}:6380"
```

### Update Container App with Redis

```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars "REDIS_URL=rediss://:${REDIS_KEY}@${REDIS_HOST}:6380"
```

## Network & Security

### CORS Configuration

Configured in `container-app-config.yaml`:

```yaml
corsPolicy:
  allowedOrigins: ['*']
  allowedMethods: [GET, POST, PUT, DELETE, OPTIONS]
  allowedHeaders: ['*']
```

### Ingress Configuration

```yaml
ingress:
  external: true
  targetPort: 8000
  additionalPortMappings:
    - external: true
      targetPort: 7071
  transport: Auto
```

### IP Restrictions

Add IP allowlist:

```bash
az containerapp ingress access-restriction set \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --rule-name "office-network" \
  --ip-address "203.0.113.0/24" \
  --action Allow
```

## Troubleshooting

### Container Won't Start

1. **Check logs**:
   ```bash
   az containerapp logs show \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --tail 200
   ```

2. **Verify image**:
   ```bash
   az acr repository show-tags \
     --name acrctalmhttps001 \
     --repository lm-http-ingest
   ```

3. **Check environment variables**:
   ```bash
   az containerapp show \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --query properties.template.containers[0].env
   ```

### Database Connection Issues

1. **Verify token is fresh**:
   ```bash
   TOKEN=$(az account get-access-token \
     --resource-type oss-rdbms \
     --query accessToken \
     --output tsv)

   # Redeploy with new token
   az containerapp update \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --set-env-vars "PGPASSWORD=$TOKEN"
   ```

2. **Check PostgreSQL firewall**:
   ```bash
   az postgres flexible-server firewall-rule list \
     --name rm-postgres \
     --resource-group CTA_Resource_Group
   ```

3. **Allow Container App IPs**:
   - Get outbound IPs from `container-app-config.yaml`
   - Add to PostgreSQL firewall rules

### Performance Issues

1. **Scale up replicas**:
   ```bash
   az containerapp update \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --min-replicas 5
   ```

2. **Increase resources**:
   ```bash
   az containerapp update \
     --name ca-cta-lm-ingest \
     --resource-group CTA_Resource_Group \
     --cpu 2.0 \
     --memory 4Gi
   ```

3. **Check metrics**:
   - CPU usage
   - Memory usage
   - Request latency
   - Active connections

## Rollback

### Rollback to Previous Revision

```bash
# List revisions
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table

# Activate previous revision
az containerapp revision activate \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision ca-cta-lm-ingest--v11
```

### Traffic Splitting (Blue-Green Deployment)

```bash
# Split traffic 50/50 between old and new
az containerapp ingress traffic set \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision-weight ca-cta-lm-ingest--v11=50 ca-cta-lm-ingest--v12=50

# Full cutover to new version
az containerapp ingress traffic set \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision-weight ca-cta-lm-ingest--v12=100
```

## Cost Optimization

### Consumption-based Pricing

Current workload profile: **Consumption**

Charges based on:
- vCPU seconds
- Memory GB-seconds
- HTTP requests

### Optimization Tips

1. **Right-size resources**: Start with 1 CPU, 2Gi RAM and scale up if needed
2. **Minimize replicas**: Use 1 min replica for dev, 2 for production
3. **Use Redis efficiently**: Enable eviction policies to limit memory
4. **Monitor usage**: Review Azure Cost Analysis regularly

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy to Azure

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Build and push to ACR
        run: |
          az acr build \
            --registry acrctalmhttps001 \
            --image lm-http-ingest:${{ github.sha }} \
            --file Dockerfile \
            .

      - name: Deploy to Container App
        run: |
          az containerapp update \
            --name ca-cta-lm-ingest \
            --resource-group CTA_Resource_Group \
            --image acrctalmhttps001.azurecr.io/lm-http-ingest:${{ github.sha }}
```

## Best Practices

1. **Always version your images**: Use semantic versioning (v1, v2, etc.)
2. **Test in staging first**: Create a separate Container App for staging
3. **Monitor health checks**: Set up alerts for failed health checks
4. **Refresh tokens regularly**: Azure AD tokens expire every 90 minutes
5. **Use managed identities**: Transition to managed identity for PostgreSQL auth
6. **Enable logging**: Send logs to Azure Monitor or Log Analytics
7. **Backup database**: Regular backups of PostgreSQL
8. **Document changes**: Update this guide with any infrastructure changes

## Next Steps

- Set up Azure Monitor alerts
- Configure Application Insights for telemetry
- Create staging environment
- Implement blue-green deployments
- Set up automated database backups
