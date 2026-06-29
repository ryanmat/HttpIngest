<div align="center">

[![tests](https://github.com/ryanmat/HttpIngest/actions/workflows/test.yml/badge.svg)](https://github.com/ryanmat/HttpIngest/actions/workflows/test.yml)
![Python](https://img.shields.io/badge/python-3.12+-blue.svg)
![License](https://img.shields.io/badge/license-MPL--2.0-blue.svg)

</div>

# HttpIngest

**HttpIngest receives OpenTelemetry (OTLP) metric payloads over HTTP and writes them to Azure Data Lake Gen2 as time-partitioned Parquet.** It decompresses gzip, parses and normalizes the OTLP JSON, buffers in memory, and flushes Hive-partitioned Parquet files for downstream analytics. It is a thin ingest-and-store pipe with no query layer.

You bring your own storage account, bearer token, and OTLP source. No credentials, endpoints, or data ship with this repository: every infrastructure value is supplied through environment variables at deploy time, and the only sample payload included is synthetic.

<table>
<tr><td><b>Async ingest</b></td><td>Non-blocking Data Lake uploads via <code>asyncio.to_thread</code>, with an in-memory buffer that flushes on a configurable interval or row count.</td></tr>
<tr><td><b>Hive-partitioned Parquet</b></td><td>Files land under <code>year=YYYY/month=MM/day=DD/hour=HH</code>, ready for partition-pruned scans by DuckDB, Spark, or Synapse.</td></tr>
<tr><td><b>Scale to zero</b></td><td>Runs on Azure Container Apps at 0 to 3 replicas, idling at zero cost and scaling out under load.</td></tr>
<tr><td><b>Passwordless auth</b></td><td>Authenticates to Data Lake with an Azure managed identity. No storage keys or connection strings are stored.</td></tr>
<tr><td><b>Bearer-gated ingest</b></td><td>Every write requires an <code>Authorization: Bearer</code> token (RFC 6750). The service fails closed when the token is unset.</td></tr>
<tr><td><b>Observable</b></td><td>OpenTelemetry tracing through any OTLP/HTTP backend and a dependency-free Prometheus <code>/metrics</code> endpoint.</td></tr>
</table>

---

## Quickstart

Run locally against your own storage account, no deployment required:

```bash
git clone https://github.com/ryanmat/HttpIngest && cd HttpIngest
uv sync

# DefaultAzureCredential (e.g. az login) authenticates to the storage account
DATALAKE_ACCOUNT=<your-storage-account> INGEST_BEARER_TOKEN=$(openssl rand -hex 32) \
  uv run uvicorn containerapp_main:app --host 0.0.0.0 --port 8000
```

Send the bundled synthetic OTLP payload:

```bash
curl -X POST localhost:8000/api/HttpIngest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $INGEST_BEARER_TOKEN" \
  -d @tests/fixtures/sample_otlp.json
```

## Configuration

Every value is supplied by environment variable; nothing is hard-coded.

| Variable | Required | Description |
|---|---|---|
| `DATALAKE_ACCOUNT` | yes | Data Lake Gen2 storage account name |
| `INGEST_BEARER_TOKEN` | yes | Shared bearer token for `/api/HttpIngest`; unset fails closed (503) |
| `USE_MANAGED_IDENTITY` | yes | Authenticate to storage with an Azure managed identity |
| `DATALAKE_FILESYSTEM` | no | Container / file-system name (default `metrics`) |
| `DATALAKE_BASE_PATH` | no | Path prefix in the container (default `otlp`) |
| `DATALAKE_FLUSH_INTERVAL_SECONDS` | no | Buffer flush interval (default `600`) |
| `DATALAKE_FLUSH_THRESHOLD_ROWS` | no | Flush when the buffer reaches this size (default `50000`) |

Generate the token with `openssl rand -hex 32` and store it as a Container App secret. See [`.env.example`](./.env.example) for the full list, including optional OpenTelemetry tracing.

## Deploy

`scripts/deploy.sh` reads its targets from the environment, so no infrastructure identifiers live in the repository:

```bash
export AZURE_REGISTRY=<your-acr-name>
export AZURE_RESOURCE_GROUP=<your-rg>
export AZURE_CONTAINER_APP=<your-container-app>

./scripts/deploy.sh v1.0.0
```

The script builds the image in your Azure Container Registry, rolls it out to the Container App, and probes the health endpoint. Full guide: [docs/deployment.md](docs/deployment.md).

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/api/HttpIngest` | Bearer | Ingest an OTLP JSON payload (gzip optional) |
| `GET` | `/health` | none | Liveness probe |
| `GET` | `/api/health` | Bearer | Component status and buffer stats |
| `GET` | `/metrics` | Bearer | Prometheus counters |

Full reference, including request and response shapes: [docs/api-documentation.md](docs/api-documentation.md).

## Data layout

Parquet files are Hive-partitioned by time, so downstream tools read the lake directly:

```
<DATALAKE_ACCOUNT>/metrics/otlp/metric_data/
  year=2026/month=04/day=30/hour=12/part-20260430120000-abc123.parquet
```

Consume with DuckDB's Azure extension, Spark, Synapse, or Databricks. Merge small hourly files into day-level files with `scripts/compact_parquet.py`. Parser internals are documented in [docs/otlp_parser.md](docs/otlp_parser.md).

## Tests

```bash
uv run pytest                 # full suite
uv run pytest --cov=src       # with coverage
```

Built with `uv`, `ruff`, and `pytest` on Python 3.12 and up.

## License

[MPL-2.0](./LICENSE).
