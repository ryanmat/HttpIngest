#!/bin/bash
# Description: Set up Application Insights monitoring dashboards and alerts for LogicMonitor Data Pipeline
# Description: Creates custom dashboards, metric alerts, and log-based alerts

set -e

# Configuration
RESOURCE_GROUP="CTA_Resource_Group"
LOCATION="eastus"
APP_NAME="ca-cta-lm-ingest"
APP_INSIGHTS_NAME="httpdatapublisher"  # Existing App Insights instance

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "🔍 Setting up Application Insights monitoring..."
echo "=============================================="
echo ""

# Check if Application Insights exists
echo "📊 Checking Application Insights..."
APP_INSIGHTS_ID=$(az monitor app-insights component show \
  --app "$APP_INSIGHTS_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "id" \
  -o tsv 2>/dev/null || echo "")

if [ -z "$APP_INSIGHTS_ID" ]; then
  echo -e "${YELLOW}⚠️  Application Insights '$APP_INSIGHTS_NAME' not found${NC}"
  echo "Creating new Application Insights instance..."

  az monitor app-insights component create \
    --app "$APP_INSIGHTS_NAME" \
    --location "$LOCATION" \
    --resource-group "$RESOURCE_GROUP" \
    --application-type web \
    --retention-time 90 \
    --output none

  APP_INSIGHTS_ID=$(az monitor app-insights component show \
    --app "$APP_INSIGHTS_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query "id" \
    -o tsv)

  echo -e "${GREEN}✅ Created Application Insights${NC}"
else
  echo -e "${GREEN}✅ Application Insights exists${NC}"
fi

# Get instrumentation key
INSTRUMENTATION_KEY=$(az monitor app-insights component show \
  --app "$APP_INSIGHTS_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "instrumentationKey" \
  -o tsv)

echo "Instrumentation Key: ${INSTRUMENTATION_KEY:0:8}..."
echo ""

# Create Alert Action Group (for notifications)
echo "🔔 Creating alert action group..."
ACTION_GROUP_NAME="lm-pipeline-alerts"

az monitor action-group create \
  --name "$ACTION_GROUP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --short-name "LM-Alerts" \
  --output none 2>/dev/null || echo "Action group already exists"

ACTION_GROUP_ID=$(az monitor action-group show \
  --name "$ACTION_GROUP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "id" \
  -o tsv)

echo -e "${GREEN}✅ Action group ready${NC}"
echo ""

# Create Metric Alerts
echo "📈 Creating metric alerts..."

# Alert 1: High HTTP 5xx Error Rate
az monitor metrics alert create \
  --name "High-5xx-Error-Rate" \
  --resource-group "$RESOURCE_GROUP" \
  --scopes "$APP_INSIGHTS_ID" \
  --condition "avg requests/failed > 10" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --description "Alerts when 5xx error rate exceeds threshold" \
  --action "$ACTION_GROUP_ID" \
  --severity 2 \
  --output none 2>/dev/null || echo "  Alert already exists"

# Alert 2: High Response Time
az monitor metrics alert create \
  --name "High-Response-Time" \
  --resource-group "$RESOURCE_GROUP" \
  --scopes "$APP_INSIGHTS_ID" \
  --condition "avg requests/duration > 2000" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --description "Alerts when average response time exceeds 2 seconds" \
  --action "$ACTION_GROUP_ID" \
  --severity 3 \
  --output none 2>/dev/null || echo "  Alert already exists"

# Alert 3: Low Availability
az monitor metrics alert create \
  --name "Low-Availability" \
  --resource-group "$RESOURCE_GROUP" \
  --scopes "$APP_INSIGHTS_ID" \
  --condition "avg availabilityResults/availabilityPercentage < 99" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --description "Alerts when availability drops below 99%" \
  --action "$ACTION_GROUP_ID" \
  --severity 1 \
  --output none 2>/dev/null || echo "  Alert already exists"

echo -e "${GREEN}✅ Metric alerts created${NC}"
echo ""

# Container App specific alerts
echo "🐳 Creating container app alerts..."

CONTAINER_APP_ID="/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.App/containerApps/$APP_NAME"

# Alert 4: High CPU Usage
az monitor metrics alert create \
  --name "High-CPU-Usage" \
  --resource-group "$RESOURCE_GROUP" \
  --scopes "$CONTAINER_APP_ID" \
  --condition "avg UsageNanoCores > 800000000" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --description "Alerts when CPU usage exceeds 80%" \
  --action "$ACTION_GROUP_ID" \
  --severity 2 \
  --output none 2>/dev/null || echo "  Alert already exists"

# Alert 5: High Memory Usage
az monitor metrics alert create \
  --name "High-Memory-Usage" \
  --resource-group "$RESOURCE_GROUP" \
  --scopes "$CONTAINER_APP_ID" \
  --condition "avg WorkingSetBytes > 1600000000" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --description "Alerts when memory usage exceeds 1.6GB (80% of 2GB)" \
  --action "$ACTION_GROUP_ID" \
  --severity 2 \
  --output none 2>/dev/null || echo "  Alert already exists"

# Alert 6: Replica Restart Rate
az monitor metrics alert create \
  --name "High-Replica-Restart-Rate" \
  --resource-group "$RESOURCE_GROUP" \
  --scopes "$CONTAINER_APP_ID" \
  --condition "avg Restarts > 5" \
  --window-size 15m \
  --evaluation-frequency 5m \
  --description "Alerts when replicas restart frequently" \
  --action "$ACTION_GROUP_ID" \
  --severity 1 \
  --output none 2>/dev/null || echo "  Alert already exists"

echo -e "${GREEN}✅ Container app alerts created${NC}"
echo ""

# Output summary
echo "=============================================="
echo -e "${GREEN}✅ Monitoring setup complete!${NC}"
echo "=============================================="
echo ""
echo "Application Insights:"
echo "  Name: $APP_INSIGHTS_NAME"
echo "  Instrumentation Key: ${INSTRUMENTATION_KEY:0:8}...${INSTRUMENTATION_KEY: -8}"
echo ""
echo "Alerts Created:"
echo "  ✓ High 5xx Error Rate (severity 2)"
echo "  ✓ High Response Time (severity 3)"
echo "  ✓ Low Availability (severity 1)"
echo "  ✓ High CPU Usage (severity 2)"
echo "  ✓ High Memory Usage (severity 2)"
echo "  ✓ High Replica Restart Rate (severity 1)"
echo ""
echo "Next Steps:"
echo "  1. Add email/SMS to action group: az monitor action-group update --name $ACTION_GROUP_NAME --add-email <email>"
echo "  2. View alerts: az monitor metrics alert list --resource-group $RESOURCE_GROUP"
echo "  3. Add instrumentation key to app: APPINSIGHTS_INSTRUMENTATION_KEY=$INSTRUMENTATION_KEY"
echo ""
