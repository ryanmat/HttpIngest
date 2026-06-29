# Description: Deployment guide for HttpIngest on Azure Container Apps.
# Description: Covers building, deploying, and managing the ADLS-only ingestion service.

# Deployment Guide

HttpIngest runs in **Data Lake-only mode**: every OTLP payload is parsed and
written to Azure Data Lake Gen2 as time-partitioned Parquet. There is no
PostgreSQL, no Synapse query layer, and no in-cluster cache.

## Prerequisites

- An Azure subscription with:
  - An Azure Container Registry (ACR)
  - An Azure Container App (already created, or create with `az containerapp create`)
  - A Data Lake Gen2 storage account
- Azure CLI logged in (`az login`)
- The Container App's managed identity granted **Storage Blob Data Contributor**
  on the Data Lake account

## Required Environment Variables (deployment)

Set these on your shell before invoking `scripts/deploy.sh`:

```bash
export AZURE_REGISTRY=<your-acr-name>          # e.g. myregistry
export AZURE_RESOURCE_GROUP=<your-rg>          # e.g. ingest-rg
export AZURE_CONTAINER_APP=<your-app>          # e.g. httpingest
# Optional:
export AZURE_IMAGE_NAME=httpingest             # default
export AZURE_HEALTH_URL=https://<your-app>.azurecontainerapps.io/api/health
```

## Required Environment Variables (Container App)

Configure these on the Container App itself. Treat the bearer token as a secret
(`az containerapp secret set` then reference it via `secretref:`):

```bash
DATALAKE_ACCOUNT=<storage-account-name>        # required
DATALAKE_FILESYSTEM=metrics                    # default: metrics
DATALAKE_BASE_PATH=otlp                        # default: otlp
USE_MANAGED_IDENTITY=true
WRITE_TO_DATALAKE=true
INGEST_BEARER_TOKEN=<32-byte-random>           # required (auth gate on /api/HttpIngest)
```

Generate the bearer token with `openssl rand -hex 32` and store it as a
Container App secret rather than a plain env var. The endpoint expects
standard RFC 6750 bearer auth (`Authorization: Bearer <token>`).

## Hardening the public ingress (recommended)

The application-layer bearer-token check is one layer. Adding ingress
restrictions or moving to internal-only ingress eliminates open-internet
exposure entirely:

```bash
# Option A: IP-restrict the public ingress to your collector's source IP
az containerapp ingress access-restriction set \
  -n "$AZURE_CONTAINER_APP" -g "$AZURE_RESOURCE_GROUP" \
  --rule-name allow-otel-collector \
  --ip-address <otel-collector-egress-ip>/32 \
  --action Allow

# Option B: Make the app VNet-only (collector must be in the same VNet)
az containerapp ingress update \
  -n "$AZURE_CONTAINER_APP" -g "$AZURE_RESOURCE_GROUP" \
  --type internal
```

## Quick Deploy

```bash
./scripts/deploy.sh v1.0.0
```

This runs three steps:
1. `az acr build --registry "$AZURE_REGISTRY" ...` — builds the image in ACR
2. `az containerapp update --image ...` — rolls out the new image
3. Optional `curl "$AZURE_HEALTH_URL"` — probes the deployed app

## OpenTelemetry Tracing

HttpIngest emits OTel spans via the standard OTLP/HTTP exporter. To enable:

```bash
OTEL_TRACING_ENABLED=true
OTEL_SERVICE_NAME=httpingest
OTEL_SERVICE_NAMESPACE=httpingest
OTEL_EXPORTER_TYPE=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://<your-otel-collector>:4318/v1/traces
# Optional auth:
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer <your-token>
OTEL_TRACES_SAMPLER_ARG=1.0
```

Set `OTEL_EXPORTER_TYPE=console` for local debugging (spans print to stdout).

### Verifying traces

```bash
az containerapp logs show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" --type console | grep "Exported"
```

Expected:

```
[otlp] Exported 3 spans successfully (total: N/N)
```

## Forwarding traces to an OTLP backend

HttpIngest emits OTLP/HTTP spans via the standard exporter, so it works with any
OTLP-compatible backend. The simplest option is to point
`OTEL_EXPORTER_OTLP_ENDPOINT` straight at your collector or vendor endpoint. To
keep backend credentials off the main app, run an OpenTelemetry Collector as a
sidecar in the same Container App: the app sends OTLP to `localhost:4318` and the
Collector forwards to your backend, holding any auth as a Container App secret.
Subsequent HttpIngest deploys via `scripts/deploy.sh` target the main container
by name (`--container-name httpingest`) and leave the sidecar untouched.

### One-time sidecar setup

**Step 1:** Store the backend credential (if any) as a Container App secret:

```bash
az containerapp secret set \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --secrets otlp-backend-token="<your-backend-token>"
```

**Step 2:** Dump the current Container App template to a YAML file:

```bash
az containerapp show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  -o yaml > /tmp/containerapp-current.yaml
```

**Step 3:** Append a Collector sidecar to `properties.template.containers`
in `/tmp/containerapp-current.yaml`, reading the secret as your chosen backend
exporter requires. The container block:

```yaml
- name: otel-collector
  image: otel/opentelemetry-collector-contrib:latest
  env:
    - name: OTLP_BACKEND_TOKEN
      secretRef: otlp-backend-token
  resources:
    cpu: 0.5
    memory: 1Gi
```

Also update the `httpingest` container's env block in the same YAML to
include the OTLP endpoint pointing at the sidecar:

```yaml
- name: OTEL_TRACING_ENABLED
  value: "true"
- name: OTEL_EXPORTER_TYPE
  value: "otlp"
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: "http://localhost:4318/v1/traces"
```

**Step 4:** Apply the updated YAML:

```bash
az containerapp update \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --yaml /tmp/containerapp-current.yaml
```

**Step 5:** Verify both containers are running:

```bash
az containerapp show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --query 'properties.template.containers[].name' -o json
# Expected: ["httpingest", "otel-collector"]
```

### Verifying spans flow

```bash
# Sidecar logs -- confirm the Collector is receiving and exporting (no errors)
az containerapp logs show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --container otel-collector --tail 30

# Main app logs -- look for "Exported N spans" indicating outbound OTLP
az containerapp logs show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --container httpingest --tail 30 | grep "Exported"
```

### Rolling back the sidecar

To remove the sidecar (preserving the main app):

```bash
# Re-dump current template, remove the otel-collector container block, re-apply
az containerapp show ... -o yaml > /tmp/rb.yaml
# edit /tmp/rb.yaml: remove the otel-collector container
az containerapp update ... --yaml /tmp/rb.yaml
```

Or activate a prior revision predating the sidecar add:

```bash
az containerapp revision list \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" -o table
az containerapp revision activate \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --revision <pre-sidecar-revision>
```

## Scaling

Default configuration: 0-3 replicas, scale-to-zero, autoscaled on HTTP
concurrency (10 concurrent requests per replica).

```bash
az containerapp update \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --min-replicas 0 --max-replicas 3
```

## Health Check

```bash
curl -s "$AZURE_HEALTH_URL" | jq .
```

Expected:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "components": {
    "datalake": { "status": "healthy" }
  }
}
```

## Logs

```bash
# Stream
az containerapp logs show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" --follow

# Recent errors
az containerapp logs show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" --tail 50 | grep ERROR
```

## Rollback

```bash
# List revisions
az containerapp revision list \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" --output table

# Activate a previous revision
az containerapp revision activate \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --revision <previous-revision-name>
```

## Troubleshooting

### `DATALAKE_ACCOUNT environment variable is required`
The application refuses to start without an explicit storage account. Set it
on the Container App before deploying.

### `403 Forbidden` writing to ADLS
The Container App's managed identity needs **Storage Blob Data Contributor**
on the target account. Grant it via:

```bash
az role assignment create \
  --assignee "$(az containerapp show -n "$AZURE_CONTAINER_APP" -g "$AZURE_RESOURCE_GROUP" --query identity.principalId -o tsv)" \
  --role "Storage Blob Data Contributor" \
  --scope "$(az storage account show -n <storage-account> --query id -o tsv)"
```

### High replica count
Check the autoscaler. Reduce `max-replicas` if your downstream is bottlenecked.

### Container status

```bash
az containerapp show \
  --name "$AZURE_CONTAINER_APP" \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --query "properties.runningStatus"
```
