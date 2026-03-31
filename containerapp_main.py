# Description: FastAPI application optimized for Azure Container Apps deployment
# Description: Provides LogicMonitor OTLP ingestion without Azure Functions runtime dependency

"""
Integrates:
- OTLP data ingestion and normalization
- Data export (Prometheus, Grafana, PowerBI, CSV/JSON)
- Background processing tasks
- Health monitoring and metrics
"""

import logging
import os
import json
import io
import gzip
import asyncio
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

# Configure logging EARLY - before any module imports that use logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

import asyncpg
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, Request
from fastapi.responses import StreamingResponse, JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.middleware.gzip import GZipMiddleware
from azure.identity.aio import DefaultAzureCredential

# Import our components
from src.exporters import (
    PrometheusExporter,
    GrafanaSimpleJSONDataSource,
    PowerBIExporter,
    CSVJSONExporter,
    TimeSeriesQuery
)
from src.otlp_parser import parse_otlp
from src.data_processor_async import AsyncDataProcessor
from src.ml_service import MLDataService, FEATURE_PROFILES, ml_api_metrics

# Import Data Lake components
from src.datalake_writer import DataLakeWriter, DataLakeConfig
from src.hot_cache_manager import HotCacheManager
from src.ingestion_router import IngestionRouter, IngestionConfig

# Import tracing (OpenTelemetry)
from src.tracing import setup_tracing, shutdown_tracing

# Synapse client is optional (requires pyodbc which needs ODBC drivers)
try:
    from src.synapse_client import SynapseClient, SynapseConfig
    SYNAPSE_AVAILABLE = True
except ImportError:
    # pyodbc not available locally (missing ODBC drivers) - will work in Azure
    SynapseClient = None  # type: ignore
    SynapseConfig = None  # type: ignore
    SYNAPSE_AVAILABLE = False

logger = logging.getLogger(__name__)

async def verify_ml_api_key(x_api_key: str = Header(None, alias="X-API-Key")) -> str:
    """Validate API key for ML endpoints. Returns the key if valid.

    When ML_API_KEY env var is not set, all requests pass (backward compatible).
    When set, requests must include a matching X-API-Key header.
    """
    ml_api_key = os.getenv("ML_API_KEY", "")
    if not ml_api_key:
        return ""
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")
    if x_api_key != ml_api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


# Rate limiting for ML endpoints
limiter = Limiter(key_func=get_remote_address)

# Global state
db_pool: Optional[asyncpg.Pool] = None
datalake_writer: Optional[DataLakeWriter] = None
hot_cache_manager: Optional[HotCacheManager] = None
ingestion_router: Optional[IngestionRouter] = None
synapse_client: Optional[SynapseClient] = None
background_tasks: Dict[str, asyncio.Task] = {}
shutdown_event = asyncio.Event()

# In-memory metrics counters for /metrics endpoint (no database dependency)
ingestion_metrics: Dict[str, Any] = {
    "requests_total": 0,
    "requests_success": 0,
    "requests_error": 0,
    "metrics_ingested": 0,
    "datalake_flushes": 0,
    "datalake_records_written": 0,
    "started_at": datetime.now().isoformat(),
}

# Feature flags
HOT_CACHE_ENABLED = os.getenv("HOT_CACHE_ENABLED", "false").lower() == "true"
SYNAPSE_ENABLED = os.getenv("SYNAPSE_ENABLED", "true").lower() == "true"

# Database configuration constants
MANAGED_IDENTITY_USER = "ca-cta-lm-ingest"


def _get_db_config() -> Dict[str, Any]:
    """Get common database configuration from environment."""
    use_managed_identity = os.getenv("USE_MANAGED_IDENTITY", "false").lower() == "true"
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "postgres"),
        "user": MANAGED_IDENTITY_USER if use_managed_identity else os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "use_managed_identity": use_managed_identity,
    }


def get_db_connection_string() -> str:
    """Get PostgreSQL connection string from environment (for psycopg2)."""
    conn_str = os.getenv("POSTGRES_CONN_STR")
    if not conn_str:
        config = _get_db_config()
        conn_str = (
            f"host={config['host']} port={config['port']} dbname={config['database']} "
            f"user={config['user']} password={config['password']} sslmode=require"
        )
    return conn_str


def get_db_connection_params() -> Dict[str, Any]:
    """Get PostgreSQL connection parameters from environment (for asyncpg)."""
    config = _get_db_config()
    return {
        "host": config["host"],
        "port": int(config["port"]),
        "database": config["database"],
        "user": config["user"],
        "password": config["password"],
        "ssl": "require"
    }

# Lifespan management for FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle management for the application.

    Handles startup and shutdown of services.
    """
    logger.info("Starting LogicMonitor Data Pipeline...")
    logger.info(f"Configuration: HOT_CACHE_ENABLED={HOT_CACHE_ENABLED}, SYNAPSE_ENABLED={SYNAPSE_ENABLED}")

    # Note: OpenTelemetry tracing is initialized at module load (before app creation)
    # to ensure FastAPI instrumentation works properly

    global db_pool, datalake_writer, hot_cache_manager, ingestion_router, synapse_client, background_tasks

    # Initialize Data Lake writer (primary storage)
    try:
        datalake_config = DataLakeConfig.from_env()
        datalake_writer = DataLakeWriter(datalake_config)
        logger.info(f"Data Lake writer initialized (account: {datalake_config.account_name})")
    except Exception as e:
        logger.error(f"Failed to initialize Data Lake writer: {e}")
        datalake_writer = None

    # Initialize Synapse client (for ML historical queries)
    if SYNAPSE_ENABLED and SYNAPSE_AVAILABLE:
        try:
            synapse_config = SynapseConfig.from_env()
            synapse_client = SynapseClient(synapse_config)
            logger.info(f"Synapse client initialized (server: {synapse_config.server})")
        except Exception as e:
            logger.error(f"Failed to initialize Synapse client: {e}")
            synapse_client = None
    elif SYNAPSE_ENABLED and not SYNAPSE_AVAILABLE:
        logger.warning("Synapse enabled but pyodbc not available - install ODBC drivers")
    else:
        logger.info("Synapse disabled - ML queries limited to hot cache")

    # Initialize PostgreSQL hot cache (optional - for dashboards)
    if HOT_CACHE_ENABLED:
        try:
            db_config = _get_db_config()

            if db_config["use_managed_identity"]:
                logger.info("Using managed identity for database authentication...")
                credential = DefaultAzureCredential()
                token = await credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
                os.environ["POSTGRES_PASSWORD"] = token.token
                logger.info("Obtained initial Azure AD token for PostgreSQL")

            db_params = get_db_connection_params()
            db_pool = await asyncpg.create_pool(
                **db_params,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database connection pool initialized (5-20 connections)")

            # Initialize hot cache manager
            hot_cache_manager = HotCacheManager(db_pool)
            logger.info("Hot cache manager initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            db_pool = None
            hot_cache_manager = None
    else:
        logger.info("Hot cache disabled - Data Lake only mode")

    # Initialize ingestion router with appropriate config
    ingestion_config = IngestionConfig(
        write_to_datalake=datalake_writer is not None,
        write_to_hot_cache=HOT_CACHE_ENABLED and db_pool is not None,
    )
    ingestion_router = IngestionRouter(
        datalake_writer=datalake_writer,
        db_pool=db_pool,
        config=ingestion_config,
    )
    logger.info(f"Ingestion router initialized (datalake={ingestion_config.write_to_datalake}, hot_cache={ingestion_config.write_to_hot_cache})")

    # Start background tasks
    try:
        # Data Lake flush task (always run if datalake enabled)
        if datalake_writer:
            background_tasks["datalake_flush"] = asyncio.create_task(datalake_flush_loop())

        # Synapse warmup task (keep serverless pool alive to avoid cold-start timeouts)
        if synapse_client and SYNAPSE_ENABLED:
            background_tasks["synapse_warmup"] = asyncio.create_task(synapse_warmup_loop())

        # Hot cache tasks (only if enabled)
        if HOT_CACHE_ENABLED and db_pool:
            background_tasks["data_processing"] = asyncio.create_task(data_processing_loop())
            background_tasks["health_monitoring"] = asyncio.create_task(health_monitoring_loop())
            background_tasks["token_refresh"] = asyncio.create_task(token_refresh_loop())
            background_tasks["hot_cache_cleanup"] = asyncio.create_task(hot_cache_cleanup_loop())

        logger.info(f"Background tasks started: {list(background_tasks.keys())}")
    except Exception as e:
        logger.error(f"Failed to start background tasks: {e}")

    yield

    # Cleanup on shutdown
    logger.info("Shutting down LogicMonitor Data Pipeline...")
    shutdown_event.set()

    # Flush Data Lake buffer before shutdown
    if datalake_writer:
        try:
            written = await datalake_writer.flush()
            logger.info(f"Final Data Lake flush: {written} records written")
        except Exception as e:
            logger.error(f"Error during final Data Lake flush: {e}")

    # Cancel background tasks
    for task_name, task in background_tasks.items():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info(f"Cancelled {task_name}")

    # Close database pool
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed")

    # Close Synapse connection
    if synapse_client:
        synapse_client.close()
        logger.info("Synapse connection closed")

    # Shutdown tracing (flush pending spans)
    shutdown_tracing()

    logger.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="LogicMonitor Data Pipeline",
    description="Unified data pipeline for LogicMonitor metrics",
    version="49.0.0",
    lifespan=lifespan
)

# Rate limiting state (must be set before routes are registered)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Response compression for large ML API payloads (500MB+ uncompressed possible)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Initialize tracing IMMEDIATELY after app creation (before routes are registered)
# This ensures FastAPI middleware is properly instrumented
setup_tracing(app)


# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def datalake_flush_loop():
    """Background task to periodically flush Data Lake buffer."""
    logger.info("Starting Data Lake flush loop...")

    flush_interval = int(os.getenv("DATALAKE_FLUSH_INTERVAL_SECONDS", "60"))

    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(flush_interval)

            if datalake_writer:
                stats = datalake_writer.get_buffer_stats()
                if stats["metric_data_buffered"] > 0:
                    written = await datalake_writer.flush()
                    ingestion_metrics["datalake_flushes"] += 1
                    ingestion_metrics["datalake_records_written"] += written
                    logger.info(f"Data Lake flush: {written} records written")

        except Exception as e:
            logger.error(f"Data Lake flush error: {e}", exc_info=True)
            await asyncio.sleep(10)


async def synapse_warmup_loop():
    """Background task to keep Synapse Serverless pool warm.

    Synapse Serverless drops idle connections and deallocates compute after
    ~5 minutes of inactivity. The first OPENROWSET query after idle requires
    a cold start (30s to 2+ minutes). This task runs a lightweight OPENROWSET
    query every 4 minutes to keep the pool warm, so real ML queries respond
    within the Container App's 240-second request timeout.

    Runs immediately on startup (no initial sleep) so the pool is warm
    before the first real ML query arrives.
    """
    logger.info("Starting Synapse warmup loop...")
    warmup_interval = int(os.getenv("SYNAPSE_WARMUP_INTERVAL_SECONDS", "240"))

    while not shutdown_event.is_set():
        try:
            if synapse_client:
                await asyncio.to_thread(_synapse_warmup_query)
            await asyncio.sleep(warmup_interval)
        except Exception as e:
            logger.warning(f"Synapse warmup query failed: {e}")
            await asyncio.sleep(30)


def _synapse_warmup_query():
    """Run a lightweight OPENROWSET query to keep Synapse compute warm."""
    if synapse_client is None:
        return
    with synapse_client._lock:
        conn = synapse_client._get_connection()
        cursor = conn.cursor()
        bulk_path = synapse_client.config.metric_data_path
        cursor.execute(
            f"SELECT TOP 1 timestamp FROM OPENROWSET("
            f"BULK '{bulk_path}', FORMAT='PARQUET'"
            f") WITH (timestamp DATETIME2) AS r"
        )
        row = cursor.fetchone()
        cursor.close()
        if row:
            logger.info(f"Synapse warmup: pool alive, latest ts={row[0]}")
        else:
            logger.info("Synapse warmup: pool alive, no data yet")


async def hot_cache_cleanup_loop():
    """Background task to clean up expired data from hot cache."""
    logger.info("Starting hot cache cleanup loop...")

    # Run cleanup every hour
    cleanup_interval = 3600

    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(cleanup_interval)

            if hot_cache_manager:
                deleted = await hot_cache_manager.cleanup_expired_data()
                total_deleted = sum(deleted.values())
                if total_deleted > 0:
                    logger.info(f"Hot cache cleanup: {total_deleted} expired records deleted")

        except Exception as e:
            logger.error(f"Hot cache cleanup error: {e}", exc_info=True)
            await asyncio.sleep(60)


async def data_processing_loop():
    """Background task to process raw OTLP data (legacy - for hot cache backfill)."""
    logger.info("Starting data processing loop...")

    while not shutdown_event.is_set():
        try:
            # Process a batch of raw metrics using async processor
            processor = AsyncDataProcessor(db_pool)

            # Use larger batch size (500) for faster processing
            stats = await processor.process_batch(limit=500)

            if stats.successful > 0:
                logger.info(
                    f"Processed {stats.successful}/{stats.total_records} metrics: "
                    f"{stats.resources_created} resources, "
                    f"{stats.datasources_created} datasources, "
                    f"{stats.metric_definitions_created} metrics, "
                    f"{stats.metric_data_created} data points"
                )

            # Only sleep if we processed fewer records than the limit (caught up)
            if stats.total_records < 500:
                await asyncio.sleep(5)
            else:
                # Small sleep to avoid overwhelming the database
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Data processing error: {e}", exc_info=True)
            await asyncio.sleep(10)


async def health_monitoring_loop():
    """Background task to monitor system health."""
    while not shutdown_event.is_set():
        try:
            # Check database connection using async pool
            async with db_pool.acquire() as conn:
                pending = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM lm_metrics lm
                    LEFT JOIN processing_status ps ON lm.id = ps.lm_metrics_id
                    WHERE ps.id IS NULL OR ps.status IN ('failed', 'processing')
                """)

            if pending > 1000:
                logger.warning(f"High pending metrics: {pending}")

            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"Health monitoring error: {e}")
            await asyncio.sleep(60)


async def token_refresh_loop():
    """Background task to refresh Azure AD tokens for PostgreSQL."""
    logger.info("Starting token refresh loop...")

    credential = DefaultAzureCredential()

    while not shutdown_event.is_set():
        try:
            # Get access token for PostgreSQL (Azure OSSRDBMS scope)
            token = await credential.get_token("https://ossrdbms-aad.database.windows.net/.default")

            logger.info("Obtained new Azure AD token for PostgreSQL")

            # Update environment variable (for sync connections used by exporters)
            os.environ["POSTGRES_PASSWORD"] = token.token

            # Recreate async connection pool with new token
            global db_pool
            if db_pool:
                await db_pool.close()
                logger.info("Closed existing database pool")

            db_params = get_db_connection_params()
            db_pool = await asyncpg.create_pool(
                **db_params,
                min_size=5,
                max_size=20,
                command_timeout=60
            )

            logger.info("Token refreshed and database pool recreated")

            # Refresh every 45 minutes (tokens last ~60-90 min)
            # This gives us a 15-45 minute buffer before expiration
            await asyncio.sleep(2700)  # 45 minutes

        except Exception as e:
            logger.error(f"Token refresh error: {e}", exc_info=True)
            # Retry more frequently on error
            await asyncio.sleep(60)


# ============================================================================
# HEALTH & INGESTION ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_root():
    """Root health check for Azure Container Apps probes."""
    return JSONResponse(content={
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": app.version,
    })


@app.get("/api/health")
async def health_check():
    """
    Health check endpoint.

    Returns status of all system components.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": app.version,
        "mode": "datalake_only" if not HOT_CACHE_ENABLED else "datalake_with_hot_cache",
        "components": {}
    }

    # Check Data Lake writer (primary storage)
    if datalake_writer:
        health_status["components"]["datalake"] = {
            "status": "healthy",
            "buffer": datalake_writer.get_buffer_stats()
        }
    else:
        health_status["components"]["datalake"] = "not initialized"
        health_status["status"] = "degraded"

    # Check hot cache (optional - for dashboards)
    if HOT_CACHE_ENABLED:
        try:
            if db_pool and hot_cache_manager:
                is_healthy = await hot_cache_manager.is_healthy()
                health_status["components"]["hot_cache"] = {
                    "status": "healthy" if is_healthy else "degraded",
                    "enabled": True
                }
            else:
                health_status["components"]["hot_cache"] = {
                    "status": "not initialized",
                    "enabled": True
                }
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["components"]["hot_cache"] = {
                "status": f"unhealthy: {str(e)}",
                "enabled": True
            }
            health_status["status"] = "degraded"
    else:
        health_status["components"]["hot_cache"] = {
            "status": "disabled",
            "enabled": False
        }

    # Check ingestion router
    if ingestion_router:
        router_status = await ingestion_router.get_status()
        health_status["components"]["ingestion_router"] = router_status
    else:
        health_status["components"]["ingestion_router"] = "not initialized"
        health_status["status"] = "degraded"

    # Check Synapse (for ML historical queries)
    if SYNAPSE_ENABLED:
        if synapse_client:
            try:
                synapse_health = await synapse_client.check_health()
                health_status["components"]["synapse"] = synapse_health
            except Exception as e:
                health_status["components"]["synapse"] = {
                    "status": f"unhealthy: {str(e)}",
                    "enabled": True
                }
        else:
            health_status["components"]["synapse"] = {
                "status": "not initialized",
                "enabled": True
            }
    else:
        health_status["components"]["synapse"] = {
            "status": "disabled",
            "enabled": False
        }

    # Check background tasks
    running_tasks = sum(1 for t in background_tasks.values() if not t.done())
    health_status["components"]["background_tasks"] = {
        "running": running_tasks,
        "total": len(background_tasks),
        "tasks": list(background_tasks.keys())
    }

    status_code = 200 if health_status["status"] == "healthy" else 503

    return JSONResponse(content=health_status, status_code=status_code)


@app.post("/api/HttpIngest")
async def http_ingest(request: Request):
    """
    LogicMonitor OTLP ingestion endpoint.

    Accepts OTLP JSON payloads and routes to Data Lake (primary) and
    optionally PostgreSQL hot cache (for dashboards).

    Data Lake: All data stored as Parquet for ML training and historical queries.
    Hot Cache: Last 48 hours for real-time Prometheus/Grafana dashboards.
    """
    try:
        # Get content type and body
        content_type = request.headers.get("content-type", "application/json")
        body = await request.body()

        # Handle gzip compression
        if "gzip" in content_type or request.headers.get("content-encoding") == "gzip":
            body = gzip.decompress(body)

        # Parse JSON
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            return JSONResponse(
                content={"error": f"Invalid JSON: {str(e)}"},
                status_code=400
            )

        # Validate OTLP structure
        if "resourceMetrics" not in payload:
            return JSONResponse(
                content={"error": "Missing resourceMetrics in OTLP payload"},
                status_code=400
            )

        # Check if ingestion router is available
        if not ingestion_router:
            logger.error("Ingestion router not initialized")
            return JSONResponse(
                content={"error": "Ingestion service not available"},
                status_code=503
            )

        # Route to Data Lake and optionally hot cache
        ingestion_metrics["requests_total"] += 1
        stats = await ingestion_router.ingest(payload)

        # Check for errors
        if stats.errors and stats.datalake_written == 0 and stats.hot_cache_written == 0:
            ingestion_metrics["requests_error"] += 1
            return JSONResponse(
                content={
                    "status": "error",
                    "errors": stats.errors,
                    "timestamp": datetime.now().isoformat()
                },
                status_code=500
            )

        ingestion_metrics["requests_success"] += 1
        ingestion_metrics["metrics_ingested"] += stats.datalake_written

        return JSONResponse(
            content={
                "status": "success",
                "stats": stats.to_dict(),
                "timestamp": datetime.now().isoformat()
            },
            status_code=200
        )

    except Exception as e:
        ingestion_metrics["requests_total"] += 1
        ingestion_metrics["requests_error"] += 1
        logger.error(f"Ingestion error: {e}", exc_info=True)
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


# ============================================================================
# EXPORT ENDPOINTS (Prometheus, Grafana, PowerBI, CSV/JSON)
# ============================================================================

# Note: Exporters are created per-request to use fresh Azure AD tokens


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint using in-memory counters."""
    buffer_stats = datalake_writer.get_buffer_stats() if datalake_writer else {}

    lines = [
        "# HELP httpingest_requests_total Total HTTP ingest requests received",
        "# TYPE httpingest_requests_total counter",
        f'httpingest_requests_total {ingestion_metrics["requests_total"]}',
        "# HELP httpingest_requests_success_total Successful ingest requests",
        "# TYPE httpingest_requests_success_total counter",
        f'httpingest_requests_success_total {ingestion_metrics["requests_success"]}',
        "# HELP httpingest_requests_error_total Failed ingest requests",
        "# TYPE httpingest_requests_error_total counter",
        f'httpingest_requests_error_total {ingestion_metrics["requests_error"]}',
        "# HELP httpingest_metrics_ingested_total Total individual metrics ingested",
        "# TYPE httpingest_metrics_ingested_total counter",
        f'httpingest_metrics_ingested_total {ingestion_metrics["metrics_ingested"]}',
        "# HELP httpingest_datalake_flushes_total Data Lake flush operations",
        "# TYPE httpingest_datalake_flushes_total counter",
        f'httpingest_datalake_flushes_total {ingestion_metrics["datalake_flushes"]}',
        "# HELP httpingest_datalake_records_written_total Records written to Data Lake",
        "# TYPE httpingest_datalake_records_written_total counter",
        f'httpingest_datalake_records_written_total {ingestion_metrics["datalake_records_written"]}',
        "# HELP httpingest_datalake_buffer_size Current Data Lake buffer size",
        "# TYPE httpingest_datalake_buffer_size gauge",
        f'httpingest_datalake_buffer_size {buffer_stats.get("metric_data_buffered", 0)}',
        "# HELP httpingest_info Application info",
        "# TYPE httpingest_info gauge",
        f'httpingest_info{{version="{app.version}",mode="{"datalake_only" if not HOT_CACHE_ENABLED else "datalake_with_hot_cache"}"}} 1',
        "# HELP httpingest_ml_requests_total Total ML API requests",
        "# TYPE httpingest_ml_requests_total counter",
        f'httpingest_ml_requests_total {ml_api_metrics["requests_total"]}',
        "# HELP httpingest_ml_query_duration_seconds_total Cumulative ML query execution time",
        "# TYPE httpingest_ml_query_duration_seconds_total counter",
        f'httpingest_ml_query_duration_seconds_total {ml_api_metrics["query_duration_seconds_total"]:.4f}',
        "# HELP httpingest_ml_rows_returned_total Total rows returned by ML queries",
        "# TYPE httpingest_ml_rows_returned_total counter",
        f'httpingest_ml_rows_returned_total {ml_api_metrics["rows_returned_total"]}',
        "# HELP httpingest_ml_errors_total Total ML API errors",
        "# TYPE httpingest_ml_errors_total counter",
        f'httpingest_ml_errors_total {ml_api_metrics["errors_total"]}',
        "",
    ]
    return Response(content="\n".join(lines), media_type="text/plain; version=0.0.4")


@app.get("/grafana/search")
async def grafana_search(request: Request):
    """Grafana SimpleJSON search endpoint. Requires hot cache (PostgreSQL)."""
    if not HOT_CACHE_ENABLED or not db_pool:
        return JSONResponse(
            content={"error": "Grafana integration requires hot cache (PostgreSQL). Currently in Data Lake only mode."},
            status_code=503
        )
    try:
        datasource = GrafanaSimpleJSONDataSource(get_db_connection_string())
        result = datasource.search()
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Grafana search error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/grafana/query")
async def grafana_query(request: Request):
    """Grafana SimpleJSON query endpoint. Requires hot cache (PostgreSQL)."""
    if not HOT_CACHE_ENABLED or not db_pool:
        return JSONResponse(
            content={"error": "Grafana integration requires hot cache (PostgreSQL). Currently in Data Lake only mode."},
            status_code=503
        )
    try:
        datasource = GrafanaSimpleJSONDataSource(get_db_connection_string())
        body = await request.json()
        result = datasource.query(body)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Grafana query error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/export/powerbi")
async def powerbi_export(
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    """Export data in PowerBI-compatible format. Requires hot cache (PostgreSQL)."""
    if not HOT_CACHE_ENABLED or not db_pool:
        return JSONResponse(
            content={"error": "PowerBI export requires hot cache (PostgreSQL). Currently in Data Lake only mode."},
            status_code=503
        )
    try:
        exporter = PowerBIExporter(get_db_connection_string())
        query = TimeSeriesQuery(
            start_time=datetime.fromisoformat(start_time) if start_time else datetime.now() - timedelta(hours=24),
            end_time=datetime.fromisoformat(end_time) if end_time else datetime.now()
        )
        result = exporter.export_data(query)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"PowerBI export error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/export/csv")
async def csv_export(
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    """Export data as CSV. Requires hot cache (PostgreSQL)."""
    if not HOT_CACHE_ENABLED or not db_pool:
        return Response(
            content="CSV export requires hot cache (PostgreSQL). Currently in Data Lake only mode.",
            media_type="text/plain", status_code=503
        )
    try:
        exporter = CSVJSONExporter(get_db_connection_string())
        query = TimeSeriesQuery(
            start_time=datetime.fromisoformat(start_time) if start_time else datetime.now() - timedelta(hours=24),
            end_time=datetime.fromisoformat(end_time) if end_time else datetime.now()
        )
        csv_data = exporter.export_csv(query)

        return Response(
            content=csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=metrics.csv"}
        )
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        return Response(content=f"Error: {str(e)}", media_type="text/plain", status_code=500)


@app.get("/export/json")
async def json_export(
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    """Export data as JSON. Requires hot cache (PostgreSQL)."""
    if not HOT_CACHE_ENABLED or not db_pool:
        return JSONResponse(
            content={"error": "JSON export requires hot cache (PostgreSQL). Currently in Data Lake only mode."},
            status_code=503
        )
    try:
        exporter = CSVJSONExporter(get_db_connection_string())
        query = TimeSeriesQuery(
            start_time=datetime.fromisoformat(start_time) if start_time else datetime.now() - timedelta(hours=24),
            end_time=datetime.fromisoformat(end_time) if end_time else datetime.now()
        )
        result = exporter.export_json(query)
        return JSONResponse(content=json.loads(result))
    except Exception as e:
        logger.error(f"JSON export error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


# ============================================================================
# ML DATA SERVICE ENDPOINTS (for Precursor integration)
# ============================================================================


@app.get("/api/ml/inventory")
@limiter.limit("10/minute")
async def ml_inventory(
    request: Request,
    datasource: Optional[str] = Query(None, description="Filter by datasource name"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    _api_key: str = Depends(verify_ml_api_key),
):
    """
    Get inventory of available metrics, resources, and time ranges.

    Returns summary of all data available for ML training, including:
    - List of metrics with data point counts
    - List of resources with hostnames
    - Available datasources
    - Time range of available data
    """
    try:
        if not db_pool and not synapse_client:
            return JSONResponse(content={"error": "No data source available"}, status_code=503)

        service = MLDataService(pool=db_pool, synapse_client=synapse_client)
        inventory = await service.get_inventory(datasource=datasource, resource_type=resource_type)

        return JSONResponse(content={
            "metrics": inventory.metrics,
            "resources": inventory.resources,
            "datasources": inventory.datasources,
            "time_range": inventory.time_range,
            "total_data_points": inventory.total_data_points,
        })
    except Exception as e:
        logger.error(f"ML inventory error: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/ml/training-data")
@limiter.limit("10/minute")
async def ml_training_data(
    request: Request,
    start_time: Optional[str] = Query(None, description="Start time (ISO 8601)"),
    end_time: Optional[str] = Query(None, description="End time (ISO 8601)"),
    profile: Optional[str] = Query(None, description="Feature profile filter"),
    resource_id: Optional[int] = Query(None, description="Resource ID filter"),
    limit: int = Query(10000, description="Maximum records to return", le=100000),
    offset: int = Query(0, description="Pagination offset"),
    _api_key: str = Depends(verify_ml_api_key),
):
    """
    Get training data in Precursor-compatible format.

    Returns metric data formatted for Precursor ML training:
    - resource_id, host_name, service_name
    - metric_name, timestamp, value
    - datasource information

    Use 'profile' parameter to filter to specific feature profiles:
    - collector: LogicMonitor Collector metrics
    - kubernetes: Container/K8s metrics
    - cloud_compute: AWS/Azure VM metrics
    - network: SNMP network device metrics
    - database: SQL/NoSQL metrics
    - application: APM metrics
    """
    try:
        logger.info(f"ML training-data: db_pool={db_pool is not None}, synapse_client={synapse_client is not None}")
        if not db_pool and not synapse_client:
            return JSONResponse(content={"error": "No data source available"}, status_code=503)

        # Validate profile if provided
        if profile and profile not in FEATURE_PROFILES:
            return JSONResponse(
                content={
                    "error": f"Unknown profile: {profile}",
                    "available_profiles": list(FEATURE_PROFILES.keys()),
                },
                status_code=400,
            )

        service = MLDataService(pool=db_pool, synapse_client=synapse_client)
        result = await service.get_training_data(
            start_time=datetime.fromisoformat(start_time) if start_time else None,
            end_time=datetime.fromisoformat(end_time) if end_time else None,
            profile=profile,
            resource_id=resource_id,
            limit=limit,
            offset=offset,
        )

        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"ML training data error: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/ml/profile-coverage")
@limiter.limit("10/minute")
async def ml_profile_coverage(
    request: Request,
    profile: Optional[str] = Query(None, description="Single profile to check"),
    _api_key: str = Depends(verify_ml_api_key),
):
    """
    Check coverage of available metrics against feature profiles.

    Returns for each profile:
    - coverage_percent: Percentage of expected metrics available
    - available: List of expected metrics that exist in database
    - missing: List of expected metrics not found in database

    Use this to understand which profiles can be trained with current data.
    """
    try:
        if not db_pool:
            return JSONResponse(
                content={"error": "Profile coverage requires hot cache (PostgreSQL)"},
                status_code=503,
            )

        # Validate profile if provided
        if profile and profile not in FEATURE_PROFILES:
            return JSONResponse(
                content={
                    "error": f"Unknown profile: {profile}",
                    "available_profiles": list(FEATURE_PROFILES.keys()),
                },
                status_code=400,
            )

        service = MLDataService(pool=db_pool, synapse_client=synapse_client)
        result = await service.get_profile_coverage(profile=profile)

        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"ML profile coverage error: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/ml/profiles")
@limiter.limit("10/minute")
async def ml_profiles(
    request: Request,
    _api_key: str = Depends(verify_ml_api_key),
):
    """
    List available feature profiles and their expected metrics.

    Returns all defined profiles with their numerical and categorical features.
    """
    return JSONResponse(content={
        "profiles": {
            name: {
                "description": profile["description"],
                "numerical_features": profile["numerical_features"],
                "categorical_features": profile["categorical_features"],
                "total_features": len(profile["numerical_features"]) + len(profile["categorical_features"]),
            }
            for name, profile in FEATURE_PROFILES.items()
        }
    })


@app.get("/api/ml/quality")
@limiter.limit("10/minute")
async def ml_quality(
    request: Request,
    profile: Optional[str] = Query(None, description="Filter by profile name"),
    hours: int = Query(24, ge=1, le=168, description="Lookback period in hours"),
    _api_key: str = Depends(verify_ml_api_key),
):
    """
    Assess data quality for ML training readiness.

    Returns quality metrics including:
    - freshness: Time since last data point per resource
    - gaps: Detected gaps in time series data (>10 min)
    - ranges: Value statistics per metric
    - summary: Overall quality score (0-100)
    """
    if db_pool is None:
        return JSONResponse(
            content={"error": "Data quality checks require hot cache (PostgreSQL)"},
            status_code=503,
        )

    try:
        service = MLDataService(pool=db_pool, synapse_client=synapse_client)
        result = await service.get_data_quality(profile=profile, hours=hours)

        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"ML quality check error: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
