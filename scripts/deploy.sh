#!/bin/bash
# Description: Automated deployment script for Azure Container App
# Description: Builds Docker image in ACR and deploys to container app (Data Lake only mode)

set -e  # Exit on error

# Configuration
REGISTRY="acrctalmhttps001"
RESOURCE_GROUP="CTA_Resource_Group"
CONTAINER_APP="ca-cta-lm-ingest"
IMAGE_NAME="httpingest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if version is provided
if [ -z "$1" ]; then
    print_error "Usage: $0 <version>"
    echo "Example: $0 v30"
    exit 1
fi

VERSION="$1"

print_status "Starting deployment for version ${VERSION}"

# Step 1: Build Docker image in Azure Container Registry
print_status "Step 1/3: Building Docker image in ACR..."
az acr build \
  --registry "${REGISTRY}" \
  --image "${IMAGE_NAME}:${VERSION}" \
  --file Dockerfile.containerapp \
  .

if [ $? -ne 0 ]; then
    print_error "ACR build failed"
    exit 1
fi

print_status "Docker image built successfully: ${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}"

# Step 2: Deploy to Azure Container App
print_status "Step 2/3: Deploying to Azure Container App..."
az containerapp update \
  --name "${CONTAINER_APP}" \
  --resource-group "${RESOURCE_GROUP}" \
  --image "${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}"

if [ $? -ne 0 ]; then
    print_error "Container app deployment failed"
    exit 1
fi

print_status "Container app updated successfully"

# Step 3: Health check
print_status "Step 3/3: Running health check..."
sleep 15  # Wait for container to start

BASE_URL="https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io"
HEALTH_URL="${BASE_URL}/api/health"

RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "${HEALTH_URL}")

if [ "$RESPONSE" == "200" ]; then
    print_status "Health check passed (HTTP ${RESPONSE})"
else
    print_warning "Health check returned HTTP ${RESPONSE}"
fi

# Get detailed health status
print_status "Fetching health status..."
curl -s "${HEALTH_URL}" | python3 -m json.tool 2>/dev/null || curl -s "${HEALTH_URL}"

# Summary
echo ""
print_status "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
print_status "Deployment completed!"
print_status "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  Version: ${GREEN}${VERSION}${NC}"
echo -e "  Image:   ${GREEN}${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}${NC}"
echo -e "  Mode:    ${GREEN}Data Lake only${NC}"
echo -e "  Health:  ${GREEN}${HEALTH_URL}${NC}"
echo ""
