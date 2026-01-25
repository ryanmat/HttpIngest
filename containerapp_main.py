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

import asyncpg
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, Response, Request
from fastapi.responses import StreamingResponse, JSONResponse
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
from src.ml_service import MLDataService, FEATURE_PROFILES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Global state
db_pool: Optional[asyncpg.Pool] = None
background_tasks: Dict[str, asyncio.Task] = {}
shutdown_event = asyncio.Event()

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

    global db_pool, background_tasks

    # Initialize database connection pool
    # If using managed identity, get token first
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
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        db_pool = None

    # Start background tasks
    try:
        background_tasks["data_processing"] = asyncio.create_task(data_processing_loop())
        background_tasks["health_monitoring"] = asyncio.create_task(health_monitoring_loop())
        background_tasks["token_refresh"] = asyncio.create_task(token_refresh_loop())
        logger.info("Background tasks started (data processing, health monitoring, token refresh)")
    except Exception as e:
        logger.error(f"Failed to start background tasks: {e}")

    yield

    # Cleanup on shutdown
    logger.info("Shutting down LogicMonitor Data Pipeline...")
    shutdown_event.set()

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

    logger.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="LogicMonitor Data Pipeline",
    description="Unified data pipeline for LogicMonitor metrics",
    version="14.0.0",
    lifespan=lifespan
)


# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def data_processing_loop():
    """Background task to process raw OTLP data."""
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

@app.get("/api/health")
async def health_check():
    """
    Health check endpoint.

    Returns status of all system components.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "13.1-no-streaming",
        "components": {}
    }

    # Check database pool
    try:
        if db_pool:
            async with db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            health_status["components"]["database"] = "healthy"
        else:
            health_status["components"]["database"] = "pool not initialized"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["components"]["database"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"

    # Check background tasks
    running_tasks = sum(1 for t in background_tasks.values() if not t.done())
    health_status["components"]["background_tasks"] = f"{running_tasks}/{len(background_tasks)} running"

    status_code = 200 if health_status["status"] == "healthy" else 503

    return JSONResponse(content=health_status, status_code=status_code)


@app.post("/api/HttpIngest")
async def http_ingest(request: Request):
    """
    LogicMonitor OTLP ingestion endpoint.

    Accepts OTLP JSON payloads and stores them for processing.
    Uses async database operations with connection pooling for optimal performance.
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

        # Check if database pool is available
        if not db_pool:
            logger.error("Database pool not initialized")
            return JSONResponse(
                content={"error": "Database not available"},
                status_code=503
            )

        # Store raw payload using async connection pool
        async with db_pool.acquire() as conn:
            metric_id = await conn.fetchval(
                """
                INSERT INTO lm_metrics (payload, ingested_at)
                VALUES ($1, $2)
                RETURNING id
                """,
                json.dumps(payload),
                datetime.now()
            )

        logger.info(f"Ingested metric {metric_id}")

        return JSONResponse(
            content={
                "status": "success",
                "id": metric_id,
                "timestamp": datetime.now().isoformat()
            },
            status_code=200
        )

    except Exception as e:
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
    """Prometheus metrics endpoint."""
    try:
        exporter = PrometheusExporter(get_db_connection_string())
        query = TimeSeriesQuery(
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now(),
            limit=1000
        )
        metrics_text = exporter.export_metrics(query)
        return Response(content=metrics_text, media_type="text/plain; version=0.0.4")
    except Exception as e:
        logger.error(f"Prometheus export error: {e}")
        return Response(content=f"# Error: {str(e)}\n", media_type="text/plain", status_code=500)


@app.get("/grafana/search")
async def grafana_search(request: Request):
    """Grafana SimpleJSON search endpoint."""
    try:
        datasource = GrafanaSimpleJSONDataSource(get_db_connection_string())
        result = datasource.search()
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Grafana search error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/grafana/query")
async def grafana_query(request: Request):
    """Grafana SimpleJSON query endpoint."""
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
    """Export data in PowerBI-compatible format."""
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
    """Export data as CSV."""
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
    """Export data as JSON."""
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
async def ml_inventory(
    datasource: Optional[str] = Query(None, description="Filter by datasource name"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
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
        if not db_pool:
            return JSONResponse(content={"error": "Database not available"}, status_code=503)

        service = MLDataService(db_pool)
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
async def ml_training_data(
    start_time: Optional[str] = Query(None, description="Start time (ISO 8601)"),
    end_time: Optional[str] = Query(None, description="End time (ISO 8601)"),
    profile: Optional[str] = Query(None, description="Feature profile filter"),
    resource_id: Optional[int] = Query(None, description="Resource ID filter"),
    limit: int = Query(10000, description="Maximum records to return", le=100000),
    offset: int = Query(0, description="Pagination offset"),
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
        if not db_pool:
            return JSONResponse(content={"error": "Database not available"}, status_code=503)

        # Validate profile if provided
        if profile and profile not in FEATURE_PROFILES:
            return JSONResponse(
                content={
                    "error": f"Unknown profile: {profile}",
                    "available_profiles": list(FEATURE_PROFILES.keys()),
                },
                status_code=400,
            )

        service = MLDataService(db_pool)
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
async def ml_profile_coverage(
    profile: Optional[str] = Query(None, description="Single profile to check"),
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
            return JSONResponse(content={"error": "Database not available"}, status_code=503)

        # Validate profile if provided
        if profile and profile not in FEATURE_PROFILES:
            return JSONResponse(
                content={
                    "error": f"Unknown profile: {profile}",
                    "available_profiles": list(FEATURE_PROFILES.keys()),
                },
                status_code=400,
            )

        service = MLDataService(db_pool)
        result = await service.get_profile_coverage(profile=profile)

        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"ML profile coverage error: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/ml/profiles")
async def ml_profiles():
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
