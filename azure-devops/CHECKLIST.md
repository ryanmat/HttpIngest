# Azure DevOps Pipeline Configuration Checklist

## Pre-Deployment Checklist

### Azure Resources
- [ ] Azure Container Registry created
- [ ] Azure Container Apps Environment created
- [ ] Azure PostgreSQL Flexible Server created
- [ ] Resource group exists
- [ ] User has Contributor or Owner role on subscription

### Azure DevOps
- [ ] Azure DevOps organization and project created
- [ ] Repository connected to Azure DevOps
- [ ] User has pipeline creation permissions

## Configuration Steps

### 1. Service Connection
- [ ] Create Azure Resource Manager service connection
- [ ] Name: `Azure-Service-Connection`
- [ ] Verify connection has access to resource group
- [ ] Test connection successful

### 2. Update Pipeline Variables

Edit `azure-devops/azure-pipelines.yml` lines 6-23:

```yaml
variables:
  azureServiceConnection: 'Azure-Service-Connection'

  # Update these values:
  acrName: '_______________'
  containerRegistry: '_______________.azurecr.io'
  imageRepository: 'lm-http-ingest'

  containerAppName: '_______________'
  resourceGroup: '_______________'
  containerAppEnvironment: '_______________'

  pgHost: '_______________.postgres.database.azure.com'
  pgDatabase: 'postgres'
  pgUser: '_______________@_______________.com'
```

- [ ] `acrName` - Your ACR name
- [ ] `containerRegistry` - Your ACR full URL
- [ ] `containerAppName` - Your Container App name
- [ ] `resourceGroup` - Your resource group
- [ ] `containerAppEnvironment` - Your Container Apps Environment
- [ ] `pgHost` - Your PostgreSQL server hostname
- [ ] `pgUser` - Your Azure AD admin email

### 3. Import Pipeline
- [ ] Navigate to Pipelines → New pipeline
- [ ] Select repository
- [ ] Choose "Existing Azure Pipelines YAML file"
- [ ] Path: `/azure-devops/azure-pipelines.yml`
- [ ] Save pipeline

### 4. First Run
- [ ] Trigger pipeline manually or commit to main
- [ ] Monitor Build stage (5-10 min)
- [ ] Monitor Deploy stage (3-5 min)
- [ ] Monitor Verify stage (1-2 min)
- [ ] Note the deployment URL from output

### 5. Configure Managed Identity (First Deployment Only)

```bash
# Get principal ID
PRINCIPAL_ID=$(az containerapp identity show \
  --name YOUR_CONTAINER_APP \
  --resource-group YOUR_RESOURCE_GROUP \
  --query principalId -o tsv)

# Add as PostgreSQL admin
az postgres flexible-server ad-admin create \
  --resource-group YOUR_RESOURCE_GROUP \
  --server-name YOUR_POSTGRES_SERVER \
  --display-name YOUR_CONTAINER_APP \
  --object-id $PRINCIPAL_ID
```

- [ ] Get managed identity principal ID
- [ ] Create PostgreSQL Azure AD admin
- [ ] Connect to PostgreSQL
- [ ] Grant database permissions:
  ```sql
  GRANT ALL PRIVILEGES ON DATABASE postgres TO "YOUR_CONTAINER_APP";
  GRANT ALL PRIVILEGES ON SCHEMA public TO "YOUR_CONTAINER_APP";
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "YOUR_CONTAINER_APP";
  GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "YOUR_CONTAINER_APP";
  ```

### 6. Verify Deployment

```bash
# Get app URL
FQDN=$(az containerapp show \
  --name YOUR_CONTAINER_APP \
  --resource-group YOUR_RESOURCE_GROUP \
  --query properties.configuration.ingress.fqdn -o tsv)

# Test endpoints
curl https://$FQDN/api/health
curl https://$FQDN/api/metrics/summary
curl https://$FQDN/metrics/prometheus?hours=1
curl https://$FQDN/grafana/
```

- [ ] Health endpoint returns `{"status": "healthy"}`
- [ ] Metrics endpoint accessible
- [ ] Prometheus endpoint accessible
- [ ] Grafana datasource endpoint accessible

### 7. Configure LogicMonitor Collector

```properties
publisher.http.enable=true
publisher.http.url=https://YOUR_APP_URL/api/HttpIngest
publisher.http.format=otlp
publisher.http.compression=gzip
publisher.http.batch.size=100
publisher.http.batch.interval=30
```

- [ ] Configure Collector HTTPS Publisher
- [ ] Set URL to your Container App `/api/HttpIngest`
- [ ] Enable gzip compression
- [ ] Restart collector
- [ ] Verify data ingestion in database

## Post-Deployment Verification

### Application Health
- [ ] Check container app logs for errors
- [ ] Verify replicas running (3 minimum)
- [ ] Check CPU and memory usage
- [ ] Verify background tasks running

### Database
- [ ] Connect to PostgreSQL
- [ ] Verify tables created (lm_metrics, resources, metric_data, etc.)
- [ ] Check data ingestion: `SELECT COUNT(*) FROM lm_metrics;`
- [ ] Verify processing: `SELECT COUNT(*) FROM processing_status WHERE status = 'success';`
- [ ] Check recent data: `SELECT COUNT(*) FROM lm_metrics WHERE received_at > NOW() - INTERVAL '5 minutes';`

### Monitoring
- [ ] Set up Application Insights (optional)
- [ ] Configure Azure Monitor alerts
- [ ] Set up cost alerts for Container Apps and ACR
- [ ] Configure notification channels (email, Teams, Slack)

## Troubleshooting Quick Reference

### Pipeline fails at Build stage
- Check service connection permissions
- Verify ACR exists and is accessible
- Check Dockerfile.containerapp exists

### Pipeline fails at Deploy stage
- Verify Container Apps Environment exists
- Check resource group name is correct
- Ensure service connection has Contributor role

### Health check fails
- Wait 30-60 seconds for container startup
- Check container logs: `az containerapp logs show`
- Verify environment variables are set correctly
- Check database connection and managed identity setup

### No data in database
- Check LogicMonitor Collector configuration
- Verify HTTPS Publisher enabled
- Check Container App ingress is external
- Review Container App logs for ingestion errors
- Verify URL is accessible from collector network

## Ongoing Maintenance

### Weekly
- [ ] Review pipeline run history
- [ ] Check for failed deployments
- [ ] Monitor ACR image count and storage

### Monthly
- [ ] Review Container App scaling metrics
- [ ] Analyze database growth rate
- [ ] Review Azure costs
- [ ] Update dependencies if needed

### As Needed
- [ ] Update pipeline variables for resource changes
- [ ] Rotate secrets and connection strings
- [ ] Update Container App image when pipeline runs
- [ ] Review and update resource limits based on usage
