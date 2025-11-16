# Azure DevOps Pipeline Setup Guide

## Prerequisites

- Azure DevOps organization and project
- Azure subscription with Owner or Contributor role
- Azure Container Registry (ACR) created
- Azure Container Apps Environment created
- Azure PostgreSQL Flexible Server created
- Permissions to create service connections in Azure DevOps

## Step 1: Create Azure Service Connection

1. Navigate to Azure DevOps project settings
2. Go to **Pipelines** → **Service connections**
3. Click **New service connection**
4. Select **Azure Resource Manager**
5. Choose **Service principal (automatic)**
6. Select your subscription and resource group
7. Name the connection: `Azure-Service-Connection`
8. Click **Save**

## Step 2: Configure Pipeline Variables

In `azure-pipelines.yml`, update the following variables:

```yaml
variables:
  # Azure Service Connection (must match Step 1)
  azureServiceConnection: 'Azure-Service-Connection'

  # Azure Container Registry
  acrName: 'your-acr-name'  # Just the name, not .azurecr.io
  containerRegistry: 'your-acr-name.azurecr.io'
  imageRepository: 'lm-http-ingest'

  # Container App Settings
  containerAppName: 'your-container-app-name'
  resourceGroup: 'your-resource-group'
  containerAppEnvironment: 'your-container-environment'

  # PostgreSQL Settings
  pgHost: 'your-postgres-server.postgres.database.azure.com'
  pgDatabase: 'postgres'
  pgUser: 'your-azuread-user@example.com'
```

### Variable Mapping

| Variable | Description | Example |
|----------|-------------|---------|
| `acrName` | Azure Container Registry name | `myacr001` |
| `containerRegistry` | Full ACR URL | `myacr001.azurecr.io` |
| `imageRepository` | Image name | `lm-http-ingest` |
| `containerAppName` | Container App resource name | `lm-ingest-app` |
| `resourceGroup` | Azure resource group | `my-resource-group` |
| `containerAppEnvironment` | Container Apps Environment name | `my-container-env` |
| `pgHost` | PostgreSQL server hostname | `mypostgres.postgres.database.azure.com` |
| `pgDatabase` | PostgreSQL database name | `postgres` |
| `pgUser` | Azure AD user for PostgreSQL | `admin@example.com` |

## Step 3: Import Pipeline to Azure DevOps

### Option A: Through Azure DevOps UI

1. Navigate to **Pipelines** → **Pipelines**
2. Click **New pipeline**
3. Select **Azure Repos Git** (or your repository location)
4. Select your repository
5. Choose **Existing Azure Pipelines YAML file**
6. Select branch: `main`
7. Path: `/azure-devops/azure-pipelines.yml`
8. Click **Continue**
9. Review the pipeline
10. Click **Run** to trigger first build

### Option B: Through Azure CLI

```bash
# Install Azure DevOps extension
az extension add --name azure-devops

# Login to Azure DevOps
az devops login

# Set default organization and project
az devops configure --defaults organization=https://dev.azure.com/your-org project=your-project

# Create pipeline
az pipelines create \
  --name "LM HTTP Ingest - Container App Deploy" \
  --description "Build and deploy LM HTTP Ingest to Azure Container Apps" \
  --repository your-repo-name \
  --branch main \
  --yml-path azure-devops/azure-pipelines.yml
```

## Step 4: Configure Branch Triggers

By default, the pipeline triggers on commits to `main` branch.

To modify triggers, edit `azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
    - main
    - release/*
  paths:
    exclude:
    - docs/*
    - README.md
```

## Step 5: First Pipeline Run

1. Make a commit to the `main` branch (or configured trigger branch)
2. Pipeline will automatically trigger
3. Monitor the pipeline run in Azure DevOps

### Expected Pipeline Stages

1. **Build** (~5-10 minutes)
   - Checkout code
   - Build Docker image
   - Push to ACR

2. **Deploy** (~3-5 minutes)
   - Update Container App with new image
   - Set environment variables
   - Configure managed identity
   - Display deployment summary

3. **Verify** (~1-2 minutes)
   - Test health endpoint
   - Test metrics endpoints
   - Test Grafana datasource
   - Test Prometheus endpoint

## Step 6: Verify Deployment

After successful pipeline run, verify the deployment:

```bash
# Get Container App URL
az containerapp show \
  --name your-container-app-name \
  --resource-group your-resource-group \
  --query properties.configuration.ingress.fqdn -o tsv

# Test health endpoint
curl https://YOUR-APP-URL/api/health

# Test ingest endpoint
curl -X POST https://YOUR-APP-URL/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'
```

## Step 7: Configure Managed Identity (First Deployment Only)

After first deployment, configure managed identity for PostgreSQL access:

```bash
# Get managed identity principal ID
PRINCIPAL_ID=$(az containerapp identity show \
  --name your-container-app-name \
  --resource-group your-resource-group \
  --query principalId -o tsv)

# Create Azure AD admin for PostgreSQL
az postgres flexible-server ad-admin create \
  --resource-group your-resource-group \
  --server-name your-postgres-server \
  --display-name your-container-app-name \
  --object-id $PRINCIPAL_ID

# Grant database permissions (connect to PostgreSQL and run)
psql -h your-postgres-server.postgres.database.azure.com -U your-admin-user -d postgres

# In PostgreSQL:
GRANT ALL PRIVILEGES ON DATABASE postgres TO "your-container-app-name";
GRANT ALL PRIVILEGES ON SCHEMA public TO "your-container-app-name";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "your-container-app-name";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "your-container-app-name";
```

## Troubleshooting

### Pipeline Fails at Build Stage

**Error**: `unauthorized: authentication required`

**Solution**: Verify service connection has access to ACR
```bash
# Grant service principal access to ACR
az role assignment create \
  --assignee SERVICE_PRINCIPAL_ID \
  --role AcrPush \
  --scope /subscriptions/SUBSCRIPTION_ID/resourceGroups/RESOURCE_GROUP/providers/Microsoft.ContainerRegistry/registries/ACR_NAME
```

### Pipeline Fails at Deploy Stage

**Error**: `Container app not found`

**Solution**: Create container app first (or let pipeline create it)

The pipeline will create the container app if it doesn't exist. Ensure:
- `containerAppEnvironment` variable points to existing environment
- Service connection has Contributor role on resource group

### Health Check Fails

**Error**: `Health check returned HTTP 503`

**Possible causes**:
1. Container still starting (wait 30-60 seconds)
2. Database connection issue
3. Environment variables misconfigured

**Debug steps**:
```bash
# Check container logs
az containerapp logs show \
  --name your-container-app-name \
  --resource-group your-resource-group \
  --tail 50

# Check environment variables
az containerapp show \
  --name your-container-app-name \
  --resource-group your-resource-group \
  --query "properties.template.containers[0].env"
```

### Managed Identity Authentication Fails

**Error**: `password authentication failed` or `no Azure Active Directory access token`

**Solution**: Verify managed identity setup
```bash
# Check managed identity is assigned
az containerapp identity show \
  --name your-container-app-name \
  --resource-group your-resource-group

# Check PostgreSQL AD admin
az postgres flexible-server ad-admin list \
  --resource-group your-resource-group \
  --server-name your-postgres-server

# Verify USE_MANAGED_IDENTITY=true
az containerapp show \
  --name your-container-app-name \
  --resource-group your-resource-group \
  --query "properties.template.containers[0].env[?name=='USE_MANAGED_IDENTITY'].value"
```

## Pipeline Customization

### Adjust Resource Limits

In `azure-pipelines.yml`, modify container resources:

```yaml
--cpu 2.0 \
--memory 4.0Gi \
--min-replicas 3 \
--max-replicas 20 \
```

### Add Environment-Specific Deployments

Add staging deployment:

```yaml
- stage: DeployStaging
  displayName: 'Deploy to Staging'
  dependsOn: Build
  condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/develop'))
  jobs:
  - job: DeployStaging
    displayName: 'Deploy to Staging Environment'
    steps:
    - task: AzureCLI@2
      # ... similar to production deploy
```

### Add Approval Gates

1. Go to **Pipelines** → **Environments**
2. Create environment: `Production`
3. Add approval check
4. Update pipeline to use environment:

```yaml
- stage: Deploy
  jobs:
  - deployment: DeployProd
    environment: Production
    strategy:
      runOnce:
        deploy:
          steps:
          # ... deployment steps
```

## Monitoring Pipeline Runs

### View Pipeline History

```bash
# List recent pipeline runs
az pipelines runs list \
  --pipeline-ids YOUR_PIPELINE_ID \
  --top 10

# Get specific run details
az pipelines runs show \
  --id RUN_ID
```

### Pipeline Analytics

Navigate to **Pipelines** → **Analytics** to view:
- Success/failure rates
- Average duration
- Test pass rates
- Deployment frequency

## Best Practices

1. **Use Variable Groups**: Store sensitive values in Azure DevOps variable groups
2. **Enable Branch Protection**: Require PR reviews before merging to main
3. **Add Smoke Tests**: Extend verify stage with actual API tests
4. **Monitor Costs**: Track ACR storage and Container App compute costs
5. **Rotate Secrets**: Use Azure Key Vault for secret management
6. **Tag Images**: Use semantic versioning for production images
7. **Backup Configuration**: Export pipeline YAML regularly

## Next Steps

- Set up staging environment pipeline
- Configure automated rollback on failed health checks
- Add performance testing stage
- Integrate with Application Insights for deployment tracking
- Set up deployment notifications (email, Teams, Slack)
