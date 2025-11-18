# Description: Azure Functions entry point for LogicMonitor OTLP data pipeline
# Description: Provides HTTP ingestion, data processing, and export endpoints

"""
LogicMonitor Data Pipeline - Unified Application

Integrates all components:
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

import psycopg2
from psycopg2.extras import RealDictCursor
import azure.functions as func
from fastapi import FastAPI, Query, Response, Request
from fastapi.responses import StreamingResponse, JSONResponse

# Import our components
from src.exporters import (
    PrometheusExporter,
    GrafanaSimpleJSONDataSource,
    PowerBIExporter,
    CSVJSONExporter,
    TimeSeriesQuery
)

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
background_tasks: Dict[str, asyncio.Task] = {}
shutdown_event = asyncio.Event()

# Database connection
def get_db_connection_string() -> str:
    """Get PostgreSQL connection string from environment."""
    conn_str = os.getenv("POSTGRES_CONN_STR")
    if not conn_str:
        # Fallback to individual components
        # Use psycopg2 parameter format instead of URL to avoid special character issues
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "postgres")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")

        conn_str = f"host={host} port={port} dbname={db} user={user} password={password} sslmode=require"

    return conn_str

# Lifespan management for FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle management for the application.

    Handles startup and shutdown of services.
    """
    logger.info("Starting LogicMonitor Data Pipeline...")

    global background_tasks

    # Start background tasks
    try:
        background_tasks["data_processor"] = asyncio.create_task(
            data_processing_loop()
        )
        background_tasks["health_monitor"] = asyncio.create_task(
            health_monitoring_loop()
        )
        logger.info("Background tasks started")
    except Exception as e:
        logger.error(f"Failed to start background tasks: {e}")

    logger.info("Application ready!")

    yield  # Application runs here

    # Shutdown
    logger.info("Shutting down gracefully...")

    # Signal shutdown
    shutdown_event.set()

    # Cancel background tasks
    for name, task in background_tasks.items():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info(f"✅ {name} cancelled")

    logger.info("👋 Shutdown complete")

# Create FastAPI app
fastapi_app = FastAPI(
    title="LogicMonitor Data Pipeline",
    description="OTLP ingestion, ML analytics, and real-time streaming",
    version="1.0.0",
    lifespan=lifespan
)

# Create Azure Functions app
app = func.FunctionApp()


# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def data_processing_loop():
    """
    Background task to process unprocessed OTLP data.

    Polls for new data and normalizes it into the schema.
    """
    logger.info("📊 Data processor started")

    # Import DataProcessor here to avoid circular imports
    from src.data_processor import DataProcessor

    while not shutdown_event.is_set():
        try:
            # Get database connection
            conn_str = get_db_connection_string()
            conn = psycopg2.connect(conn_str)

            # Create processor and process batch
            processor = DataProcessor(conn)
            stats = processor.process_batch(limit=100)

            if stats.successful > 0:
                logger.info(
                    f"✅ Processed {stats.successful} records: "
                    f"{stats.resources_created} resources, "
                    f"{stats.datasources_created} datasources, "
                    f"{stats.metric_definitions_created} metric definitions, "
                    f"{stats.metric_data_created} data points"
                )

            if stats.failed > 0:
                logger.warning(f"⚠️  {stats.failed} records failed processing")
                for error in stats.errors[:5]:  # Log first 5 errors
                    logger.warning(f"  {error}")

            conn.close()

        except Exception as e:
            logger.error(f"Error in data processor: {e}", exc_info=True)

        # Wait before next check
        await asyncio.sleep(30)  # Check every 30 seconds

async def health_monitoring_loop():
    """
    Background task to monitor system health.

    Checks database, streaming, and other services.
    """
    logger.info("💚 Health monitor started")

    while not shutdown_event.is_set():
        try:
            health_status = {}

            # Check database
            try:
                conn = psycopg2.connect(get_db_connection_string())
                conn.close()
                health_status["database"] = "healthy"
            except:
                health_status["database"] = "unhealthy"

            # Log health status
            unhealthy = [k for k, v in health_status.items() if v == "unhealthy"]
            if unhealthy:
                logger.warning(f"⚠️  Unhealthy components: {unhealthy}")

        except Exception as e:
            logger.error(f"Error in health monitor: {e}")

        await asyncio.sleep(60)  # Check every minute


# ============================================================================
# AZURE FUNCTIONS ENDPOINTS
# ============================================================================

@app.function_name(name="HttpIngest")
@app.route(route="HttpIngest", methods=["POST"])
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
    """
    OTLP data ingestion endpoint.

    Accepts gzipped or plain JSON OTLP data from LogicMonitor.
    """
    logger.info('Processing OTLP data from LogicMonitor')

    try:
        # Read and decompress if needed
        body_bytes = req.get_body()

        if req.headers.get("Content-Encoding", "").lower() == "gzip":
            logger.info("Decompressing gzipped payload")
            with gzip.GzipFile(fileobj=io.BytesIO(body_bytes)) as f:
                decompressed_bytes = f.read()
            req_body = json.loads(decompressed_bytes.decode('utf-8'))
        else:
            req_body = req.get_json()

    except Exception as e:
        logger.error(f"Error parsing payload: {e}")
        return func.HttpResponse("Invalid payload", status_code=400)

    # Store in lm_metrics table for processing
    conn_str = get_db_connection_string()
    if not conn_str:
        return func.HttpResponse("Database not configured", status_code=500)

    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        # Insert into lm_metrics table
        insert_query = """
            INSERT INTO lm_metrics (payload)
            VALUES (%s)
            RETURNING id
        """
        cursor.execute(
            insert_query,
            (json.dumps(req_body),)
        )
        inserted_id = cursor.fetchone()[0]
        conn.commit()

        cursor.close()
        conn.close()

        logger.info(f"✅ Inserted metric batch {inserted_id}")
        return func.HttpResponse(
            json.dumps({"id": inserted_id, "status": "accepted"}),
            status_code=202,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Database error: {e}")
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)


@app.function_name(name="Health")
@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint.

    Returns status of all system components.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {}
    }

    # Check database
    try:
        conn = psycopg2.connect(get_db_connection_string())
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        health_status["components"]["database"] = "healthy"
    except Exception as e:
        health_status["components"]["database"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"

    # Check background tasks
    running_tasks = sum(1 for t in background_tasks.values() if not t.done())
    health_status["components"]["background_tasks"] = f"{running_tasks}/{len(background_tasks)} running"

    status_code = 200 if health_status["status"] == "healthy" else 503

    return func.HttpResponse(
        json.dumps(health_status, indent=2),
        status_code=status_code,
        mimetype="application/json"
    )


# ============================================================================
# FASTAPI ENDPOINTS (Exporters & Streaming)
# ============================================================================

# Initialize exporters
db_conn_str = get_db_connection_string()
prometheus_exporter = PrometheusExporter(db_conn_str)
grafana_datasource = GrafanaSimpleJSONDataSource(db_conn_str)
powerbi_exporter = PowerBIExporter(db_conn_str)
csv_json_exporter = CSVJSONExporter(db_conn_str)

# Prometheus
@fastapi_app.get("/metrics/prometheus")
async def prometheus_metrics(
    metrics: Optional[str] = Query(None, description="Comma-separated metric names"),
    hours: int = Query(1, description="Hours of data")
):
    """Export metrics in Prometheus format."""
    metric_list = metrics.split(",") if metrics else None

    query = TimeSeriesQuery(
        metric_names=metric_list,
        start_time=datetime.now() - timedelta(hours=hours),
        end_time=datetime.now()
    )

    output = prometheus_exporter.export_metrics(query)
    return Response(content=output, media_type="text/plain; version=0.0.4")

# Grafana SimpleJSON
@fastapi_app.get("/grafana")
async def grafana_health():
    """Grafana datasource health check."""
    return grafana_datasource.health_check()

@fastapi_app.post("/grafana/search")
async def grafana_search(request: Request):
    """Grafana metric search."""
    data = await request.json()
    target = data.get("target", "")
    return grafana_datasource.search(target if target else None)

@fastapi_app.post("/grafana/query")
async def grafana_query(request: Request):
    """Grafana time-series query."""
    data = await request.json()
    return grafana_datasource.query(data)

# PowerBI
@fastapi_app.get("/api/odata/metrics")
async def powerbi_metrics(
    skip: int = Query(0, ge=0),
    top: int = Query(1000, le=10000)
):
    """PowerBI OData endpoint."""
    query = TimeSeriesQuery(
        start_time=datetime.now() - timedelta(days=7)
    )
    return powerbi_exporter.export_data(query, skip=skip, top=top)

# CSV/JSON Export
@fastapi_app.get("/export/csv")
async def export_csv(
    metrics: str = Query(..., description="Comma-separated metrics"),
    hours: int = Query(24)
):
    """Export metrics as CSV."""
    query = TimeSeriesQuery(
        metric_names=metrics.split(","),
        start_time=datetime.now() - timedelta(hours=hours)
    )

    csv_data = csv_json_exporter.export_csv(query)

    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=metrics.csv"}
    )

@fastapi_app.get("/export/json")
async def export_json(
    metrics: str = Query(...),
    hours: int = Query(24),
    pretty: bool = Query(False)
):
    """Export metrics as JSON."""
    query = TimeSeriesQuery(
        metric_names=metrics.split(","),
        start_time=datetime.now() - timedelta(hours=hours)
    )

    json_data = csv_json_exporter.export_json(query, pretty=pretty)
    return Response(content=json_data, media_type="application/json")

# Health and Metrics
@fastapi_app.get("/api/health")
async def api_health():
    """Comprehensive health check."""
    health = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "components": {}
    }

    # Database
    try:
        conn = psycopg2.connect(get_db_connection_string())
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM metric_data")
            count = cur.fetchone()[0]
        conn.close()
        health["components"]["database"] = {
            "status": "healthy",
            "metric_count": count
        }
    except Exception as e:
        health["components"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health["status"] = "degraded"

    # Background tasks
    task_status = {}
    for name, task in background_tasks.items():
        task_status[name] = "running" if not task.done() else "stopped"
    health["components"]["background_tasks"] = task_status

    return health

@fastapi_app.get("/api/metrics/summary")
async def metrics_summary():
    """Get metrics summary statistics."""
    try:
        conn = psycopg2.connect(get_db_connection_string())
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get overall stats
            cur.execute("""
                SELECT
                    COUNT(DISTINCT md.id) as metric_count,
                    COUNT(DISTINCT r.id) as resource_count,
                    COUNT(*) as datapoint_count,
                    MIN(m.timestamp) as oldest_datapoint,
                    MAX(m.timestamp) as newest_datapoint
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN resources r ON m.resource_id = r.id
            """)
            stats = cur.fetchone()

        conn.close()

        return {
            "metrics": stats['metric_count'],
            "resources": stats['resource_count'],
            "total_datapoints": stats['datapoint_count'],
            "oldest_data": stats['oldest_datapoint'].isoformat() if stats['oldest_datapoint'] else None,
            "newest_data": stats['newest_datapoint'].isoformat() if stats['newest_datapoint'] else None
        }
    except Exception as e:
        return JSONResponse(
            {"error": str(e)},
            status_code=500
        )


# Mount FastAPI to Azure Functions (for local testing)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)
