#!/bin/bash
# Description: Build and deploy HttpIngest to Azure Container Apps
# Description: Reads deployment targets from environment variables (no secrets in repo)

set -euo pipefail

# Required environment variables
REGISTRY="${AZURE_REGISTRY:-}"
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-}"
CONTAINER_APP="${AZURE_CONTAINER_APP:-}"
IMAGE_NAME="${AZURE_IMAGE_NAME:-httpingest}"
HEALTH_URL="${AZURE_HEALTH_URL:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

require_env() {
    local missing=0
    for var in AZURE_REGISTRY AZURE_RESOURCE_GROUP AZURE_CONTAINER_APP; do
        if [ -z "${!var:-}" ]; then
            print_error "Missing required env var: $var"
            missing=1
        fi
    done
    if [ "$missing" = "1" ]; then
        echo
        echo "Required environment variables (set these before running):"
        echo "  AZURE_REGISTRY        - Azure Container Registry name"
        echo "  AZURE_RESOURCE_GROUP  - Resource group containing the Container App"
        echo "  AZURE_CONTAINER_APP   - Container App name to update"
        echo
        echo "Optional:"
        echo "  AZURE_IMAGE_NAME      - Image name (default: httpingest)"
        echo "  AZURE_HEALTH_URL      - URL to probe after deploy (skipped if unset)"
        exit 1
    fi
}

if [ $# -lt 1 ]; then
    print_error "Usage: $0 <version>"
    echo "Example: $0 v1.0.0"
    exit 1
fi

VERSION="$1"
require_env

print_status "Starting deployment for version ${VERSION}"

# Step 1: Build image in ACR
print_status "Step 1/3: Building Docker image in ACR..."
az acr build \
    --registry "${REGISTRY}" \
    --image "${IMAGE_NAME}:${VERSION}" \
    --file Dockerfile.containerapp \
    .
print_status "Built ${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}"

# Step 2: Update Container App
# Targets the main "httpingest" container by name so any sidecar (e.g. an
# OpenTelemetry Collector, see docs/deployment.md "Forwarding traces to an
# OTLP backend") is preserved across deploys. Safe on single-container apps
# too: the default container name matches the image name.
print_status "Step 2/3: Deploying to Azure Container App..."
az containerapp update \
    --name "${CONTAINER_APP}" \
    --resource-group "${RESOURCE_GROUP}" \
    --container-name "${IMAGE_NAME}" \
    --image "${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}"
print_status "Container app updated"

# Step 3: Optional health probe
print_status "Step 3/3: Health check"
if [ -n "$HEALTH_URL" ]; then
    sleep 15
    RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "${HEALTH_URL}")
    if [ "$RESPONSE" = "200" ]; then
        print_status "Health check passed (HTTP ${RESPONSE})"
    else
        print_warning "Health check returned HTTP ${RESPONSE}"
    fi
    curl -s "${HEALTH_URL}" | python3 -m json.tool 2>/dev/null || curl -s "${HEALTH_URL}"
else
    print_status "AZURE_HEALTH_URL not set; skipping post-deploy probe"
fi

echo
print_status "Deployment completed: ${REGISTRY}.azurecr.io/${IMAGE_NAME}:${VERSION}"
