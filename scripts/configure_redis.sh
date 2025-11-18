#!/bin/bash
# Description: Configuration script to update Container App with Redis connection details
# Description: Run after Redis instance is provisioned

set -e

echo "🔧 Configuring Redis for LogicMonitor Data Pipeline"
echo "=================================================="
echo ""

# Get Redis connection details
echo "📡 Fetching Redis connection details..."

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

REDIS_SSL_PORT=$(az redis show \
  --name lm-data-pipeline-redis \
  --resource-group CTA_Resource_Group \
  --query sslPort \
  --output tsv)

echo "✅ Redis Host: $REDIS_HOST"
echo "✅ Redis SSL Port: $REDIS_SSL_PORT"
echo ""

# Construct connection string (using rediss:// for SSL)
REDIS_URL="rediss://:${REDIS_KEY}@${REDIS_HOST}:${REDIS_SSL_PORT}"

echo "📝 Updating Container App environment variables..."
az containerapp update \
  --name ca-cta-lm-ingest \
  --resource-group CTA_Resource_Group \
  --set-env-vars \
    "REDIS_URL=${REDIS_URL}" \
    "USE_REDIS=true" \
  --output none

echo ""
echo "✅ Container App updated with Redis configuration"
echo ""
echo "=================================================="
echo "Redis Configuration Complete!"
echo "=================================================="
echo ""
echo "Connection Details:"
echo "  Host: $REDIS_HOST"
echo "  Port: $REDIS_SSL_PORT (SSL)"
echo "  Connection String: rediss://***@$REDIS_HOST:$REDIS_SSL_PORT"
echo ""
echo "Next steps:"
echo "  1. Deploy new version: ./scripts/deploy.sh v12 feature/production-redesign"
echo "  2. Verify streaming: curl https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health"
echo ""
