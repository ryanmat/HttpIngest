# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Azure Function HTTP trigger that receives gzip-compressed JSON data from LogicMonitor Collector HTTPS Data Publisher and stores it in Azure PostgreSQL Flexible Server. The function handles decompression and inserts raw JSON into a JSONB column for later querying with Power BI or other analytics tools.

## Architecture

### Current Implementation
The project currently has a **single implementation** that exists in two locations:
- [function_app.py](function_app.py) - Root level (currently identical to src/)
- [src/function_app.py](src/function_app.py) - Used by Docker builds

Both files are identical and provide basic HTTP ingestion functionality.

### Function Behavior
- **Endpoint**: POST to `/api/HttpIngest`
- **Expected Headers**:
  - `Content-Type: application/json`
  - `Content-Encoding: gzip` (for compressed payloads)
- **Processing**: Automatically detects and decompresses gzip payloads, inserts JSON into PostgreSQL
- **Authentication**: Uses connection string from `POSTGRES_CONN_STR` environment variable

## Local Development

### Prerequisites
- Python 3.11
- Azure Functions Core Tools
- PostgreSQL client (psycopg2-binary)

### Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure database connection
# Edit local.settings.json and set POSTGRES_CONN_STR
```

### Running Locally

```bash
# Start Azure Functions host
func host start

# Or use VS Code task: "func: host start"
```

The VS Code task automatically runs `pip install -r requirements.txt` before starting.

### Testing Locally

```bash
# Test with uncompressed JSON
curl -X POST http://localhost:7071/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'

# Test with gzip-compressed JSON (matches LogicMonitor behavior)
echo '{"test": "data"}' | gzip | curl -X POST http://localhost:7071/api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @-
```

## Database Setup

Create the target table in Azure PostgreSQL Flexible Server:

```sql
CREATE TABLE json_data (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL
);
```

The function inserts the entire JSON payload into the `data` column and returns the inserted row ID.

## Environment Variables

### Required

- `POSTGRES_CONN_STR`: PostgreSQL connection string
  - Format: `host=<host> dbname=<db> user=<user> password=<password>`
- `FUNCTIONS_WORKER_RUNTIME`: Set to `python`
- `AzureWebJobsStorage`: Storage connection (use `UseDevelopmentStorage=true` for local development)

## Docker Build

Multi-stage build for optimized image size:

```bash
# Build image
docker build -t lm-http-ingest .
```

**Important**: The Dockerfile uses [src/requirements.txt](src/requirements.txt) which includes `azure-identity` (not used in current code but present for future Azure AD auth). The root [requirements.txt](requirements.txt) does not include this dependency.

## Azure Deployment via CI/CD

The [azure-pipelines.yml](azure-devops/azure-pipelines.yml) pipeline handles deployment:

### Build Stage

- Builds Docker image from [Dockerfile](Dockerfile) (which uses `src/` code)
- Pushes to ACR: `acrctalmhttps001.azurecr.io/lm-http-ingest`
- Tags: `$(Build.BuildId)` and `latest-$(Build.SourceBranchName)`

### Deploy Stage

- Deploys to Azure Container Apps: `ca-cta-lm-ingest`
- Resource Group: `CTA_Resource_Group`
- Environment: `cae-cta-lm-ingest`
- Scaling: 1-10 replicas (0.5 CPU, 1.0Gi memory)

### Branch-Based Configuration

The pipeline sets environment variables based on branch:

- **main branch**: `LEGACY_MODE=true`, `USE_AZURE_AD_AUTH=false`, `ENABLE_NEW_SCHEMA=false`
- **feature/production-redesign**: `LEGACY_MODE=false`, `USE_AZURE_AD_AUTH=true`, `ENABLE_NEW_SCHEMA=true`

**Note**: The current codebase does not implement these features. These variables are placeholders for planned enhancements.

## LogicMonitor Configuration

In LogicMonitor Collector Publisher configuration:

- Set `publisher.http.url` to your deployed function URL with function key
- Example: `https://<container-app-fqdn>/api/HttpIngest?code=<function-key>`
- The Collector automatically sends data with gzip encoding

## Power BI Integration

Since data is stored as JSONB in PostgreSQL:

1. In Power BI Desktop: Get Data > PostgreSQL Database
2. Connect to: `rm-postgres.postgres.database.azure.com`
3. Query the `json_data` table
4. Use Power Query to extract fields from the JSONB `data` column

## Important Notes

- **DO NOT include `azure-functions-worker` in requirements.txt** - it's managed by Azure Functions platform (see comment in [requirements.txt](requirements.txt))
- The `src/` and root-level code are currently identical
- Azurite files (`__azurite_db_*.json`) are for local development storage and should not be committed
- The pipeline includes a health check that tests `/api/health` endpoint, but this endpoint does not exist in the current code
