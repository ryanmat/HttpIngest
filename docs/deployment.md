# Deployment Guide

**Production System:** Azure Container App receiving live LogicMonitor OTLP data

## Overview

This project deploys to Azure Container Apps using Azure Container Registry (ACR) for Docker image builds. Database authentication uses Azure AD tokens (90-minute expiration).

## Infrastructure

**Azure Resources:**
- **Container Registry:** `acrctalmhttps001`
- **Container App:** `ca-cta-lm-ingest`
- **Resource Group:** `CTA_Resource_Group`
- **Database:** Azure PostgreSQL 17.5 with Azure AD authentication
- **Current Version:** v10

**Endpoints:**
- Health: `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health`
- Ingest: `https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest`

## Deployment Methods

### Method 1: Automated Script (Recommended)

Use the deployment script for one-command deployment:

```bash
# Deploy latest code from feature branch
./scripts/deploy.sh v11

# Deploy from specific branch
./scripts/deploy.sh v11 main

# Deploy from feature branch (default)
./scripts/deploy.sh v11 feature/production-redesign
```

**What it does:**
1. Builds Docker image in ACR from GitHub repository
2. Gets fresh Azure AD access token
3. Deploys to Container App with new image and token
4. Runs health check
5. Displays deployment summary

**Prerequisites:**
- Azure CLI installed and logged in (`az login`)
- Access to resource group and container registry
- Executable permissions (`chmod +x scripts/deploy.sh`)

### Method 2: GitHub Actions (Automated CI/CD)

GitHub Actions automatically deploys on push to `feature/production-redesign` or `main`:

**Automatic Deployment:**
```bash
git push origin feature/production-redesign
# GitHub Actions triggers automatically
```

**Manual Trigger:**
1. Go to GitHub → Actions → "Deploy to Azure Container App"
2. Click "Run workflow"
3. Enter version (e.g., v11)
4. Click "Run workflow"

**Setup Required:**
Add Azure credentials as GitHub secret `AZURE_CREDENTIALS`:

```bash
# Create service principal
az ad sp create-for-rbac \
  --name "github-actions-lm-ingest" \
  --role contributor \
  --scopes /subscriptions/{subscription-id}/resourceGroups/CTA_Resource_Group \
  --sdk-auth
```

Copy the JSON output and add to GitHub:
- Repository → Settings → Secrets and variables → Actions
- New repository secret: `AZURE_CREDENTIALS`
- Paste the JSON

### Method 3: Manual Deployment

For complete control or troubleshooting:

#### Step 1: Build Docker Image

```bash
az acr build \
  --registry acrctalmhttps001 \
  --resource-group CTA_Resource_Group \
  --image lm-http-ingest:v11 \
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

#### Step 3: Deploy to Container App

```bash
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --image acrctalmhttps001.azurecr.io/lm-http-ingest:v11 \
  --set-env-vars "PGPASSWORD=$TOKEN" \
  --revision-suffix v11
```

#### Step 4: Verify Deployment

```bash
# Check health endpoint
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health

# Check container app status
az containerapp revision list \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --output table
```

## Testing Deployment

### Health Check

```bash
curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health
```

**Expected Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-14T20:00:00Z"
}
```

### Database Connectivity

```bash
# Query metrics count
curl "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/metrics?page_size=10"

# Check resources
curl "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/resources?page=1"
```

### Direct Database Query

```bash
# Connect to PostgreSQL
az postgres flexible-server connect \
  --name rm-postgres \
  --database postgres \
  --admin-user ryan.matuszewski@logicmonitor.com \
  --admin-password "$TOKEN"

# Query lm_metrics
SELECT COUNT(*) FROM lm_metrics;
SELECT * FROM lm_metrics ORDER BY created_at DESC LIMIT 5;
```

## Rollback

If deployment fails, rollback to previous revision:

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
  --revision <previous-revision-name>
```

## Troubleshooting

### Deployment Fails

**Check logs:**
```bash
az containerapp logs show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --follow
```

**Check revision status:**
```bash
az containerapp revision show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --revision <revision-name>
```

### Token Expiration

Azure AD tokens expire after 90 minutes. If you see authentication errors:

```bash
# Get fresh token
TOKEN=$(az account get-access-token \
  --resource-type oss-rdbms \
  --query accessToken \
  --output tsv)

# Redeploy with new token
./scripts/deploy.sh v11
```

### Build Fails

**Check ACR build logs:**
```bash
az acr task logs \
  --registry acrctalmhttps001 \
  --name <build-name>
```

**Common issues:**
- Dockerfile syntax errors
- Missing dependencies in requirements
- GitHub branch doesn't exist
- ACR permissions

## Version Management

**Version Naming Convention:**
- Manual deployments: `v<number>` (e.g., v10, v11)
- GitHub Actions auto: `v<YYYYMMDD-HHMM>-<sha>` (e.g., v20251114-2030-a1b2c3d)

**Current Production Version:** v10

**Versioning Strategy:**
- Increment version for each deployment
- Use semantic versioning for major changes
- Tag releases in git that correspond to deployed versions

## Monitoring

**Container App Metrics:**
```bash
az containerapp show \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --query "properties.configuration.ingress"
```

**Database Metrics:**
```bash
# Check connection count
SELECT count(*) FROM pg_stat_activity;

# Check table sizes
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Security Notes

- Azure AD tokens used for database authentication (not passwords)
- Tokens expire after 90 minutes
- Container App uses managed identity for ACR access
- No credentials stored in code or git
- Production URLs and credentials excluded from git (see .gitignore)

## Deployment Checklist

Before deploying to production:

- [ ] All tests passing locally
- [ ] Code reviewed and approved
- [ ] Feature branch merged to main (if production deployment)
- [ ] Database migrations tested
- [ ] Backup created (if schema changes)
- [ ] Version number incremented
- [ ] Health check endpoint working
- [ ] Rollback plan ready
- [ ] Team notified of deployment

---

**Last Updated:** 2025-11-14
**Maintainer:** Ryan Matuszewski
