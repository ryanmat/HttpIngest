#!/bin/bash
# ABOUTME: Automated deployment script for Azure Container App
# ABOUTME: Builds Docker image in ACR and deploys to container app with fresh Azure AD token

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
BRANCH="${2:-feature/production-redesign}"

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

# Step 4: Health check
print_status "Step 4/4: Running health check..."
sleep 5  # Wait for container to start

HEALTH_URL="https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/health"
print_status "Checking health endpoint: ${HEALTH_URL}"

HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "${HEALTH_URL}")

if [ "$HEALTH_RESPONSE" == "200" ]; then
    print_status "✓ Health check passed (HTTP ${HEALTH_RESPONSE})"
else
    print_warning "⚠ Health check returned HTTP ${HEALTH_RESPONSE}"
fi

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
