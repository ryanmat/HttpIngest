#!/bin/bash
# Description: Automated deployment script for Azure Container App
# Description: Builds Docker image in ACR and deploys to container app with fresh Azure AD token

set -e  # Exit on error

# Configuration
REGISTRY="acrctalmhttps001"
RESOURCE_GROUP="CTA_Resource_Group"
CONTAINER_APP="ca-cta-lm-ingest"
IMAGE_NAME="lm-http-ingest"
GITHUB_REPO="https://github.com/ryanmat/HttpIngest.git"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
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
    print_error "Usage: $0 <version> [branch]"
    echo "Example: $0 v11"
    echo "Example: $0 v11 feature/production-redesign"
    exit 1
fi

VERSION="$1"
BRANCH="${2:-main}"

print_status "Starting deployment for version ${VERSION} from branch ${BRANCH}"

# Step 1: Build Docker image in Azure Container Registry
print_status "Step 1/4: Building Docker image in ACR..."
az acr build \
  --registry "${REGISTRY}" \
  --resource-group "${RESOURCE_GROUP}" \
  --image "${IMAGE_NAME}:${VERSION}" \
  --file Dockerfile \
  "${GITHUB_REPO}#${BRANCH}"

if [ $? -ne 0 ]; then
    print_error "ACR build failed"
    exit 1
fi

print_status "Docker image built successfully: ${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}"

# Step 2: Get fresh Azure AD access token
print_status "Step 2/4: Getting fresh Azure AD access token..."
TOKEN=$(az account get-access-token \
  --resource-type oss-rdbms \
  --query accessToken \
  --output tsv)

if [ -z "$TOKEN" ]; then
    print_error "Failed to get Azure AD token"
    exit 1
fi

print_status "Azure AD token acquired (expires in 90 minutes)"

# Step 3: Deploy to Azure Container App
print_status "Step 3/4: Deploying to Azure Container App..."
az containerapp update \
  --name "${CONTAINER_APP}" \
  --resource-group "${RESOURCE_GROUP}" \
  --image "${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}" \
  --set-env-vars "PGPASSWORD=$TOKEN" \
  --revision-suffix "${VERSION//[^a-zA-Z0-9]/-}"  # Replace non-alphanumeric with dash

if [ $? -ne 0 ]; then
    print_error "Container app deployment failed"
    exit 1
fi

print_status "Container app updated successfully"

# Step 4: Health checks
print_status "Step 4/4: Running health checks..."
sleep 15  # Wait for container to start and initialize all services

BASE_URL="https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io"

# Check FastAPI health endpoint
print_status "Checking FastAPI health endpoint..."
FASTAPI_HEALTH_URL="${BASE_URL}/api/health"
FASTAPI_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "${FASTAPI_HEALTH_URL}")

if [ "$FASTAPI_RESPONSE" == "200" ]; then
    print_status "✓ FastAPI health check passed (HTTP ${FASTAPI_RESPONSE})"
else
    print_warning "⚠ FastAPI health check returned HTTP ${FASTAPI_RESPONSE}"
fi

# Check Azure Functions health endpoint
print_status "Checking Azure Functions health endpoint..."
AF_HEALTH_URL="${BASE_URL}/api/health"
AF_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "${AF_HEALTH_URL}")

if [ "$AF_RESPONSE" == "200" ]; then
    print_status "✓ Azure Functions health check passed (HTTP ${AF_RESPONSE})"
else
    print_warning "⚠ Azure Functions health check returned HTTP ${AF_RESPONSE}"
fi

# Check Prometheus metrics endpoint
print_status "Checking Prometheus metrics endpoint..."
METRICS_URL="${BASE_URL}/metrics/prometheus"
METRICS_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "${METRICS_URL}")

if [ "$METRICS_RESPONSE" == "200" ]; then
    print_status "✓ Metrics endpoint available (HTTP ${METRICS_RESPONSE})"
else
    print_warning "⚠ Metrics endpoint returned HTTP ${METRICS_RESPONSE}"
fi

# Get detailed health status
print_status "Fetching detailed health status..."
HEALTH_DETAILS=$(curl -s "${FASTAPI_HEALTH_URL}")
echo "$HEALTH_DETAILS" | python3 -m json.tool 2>/dev/null || echo "$HEALTH_DETAILS"

# Summary
echo ""
print_status "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
print_status "Deployment completed successfully!"
print_status "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  Version:       ${GREEN}${VERSION}${NC}"
echo -e "  Branch:        ${GREEN}${BRANCH}${NC}"
echo -e "  Image:         ${GREEN}${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}${NC}"
echo -e "  Container App: ${GREEN}${CONTAINER_APP}${NC}"
echo -e "  Health:        ${GREEN}${HEALTH_URL}${NC}"
echo ""
print_warning "Note: Azure AD token expires in 90 minutes. Redeploy if needed."
echo ""
