# Description: FastAPI application for Azure Container Apps OTLP ingestion
# Description: Ingests OTLP metrics and writes them to Azure Data Lake Gen2 as Parquet

"""
HttpIngest - OTLP ingestion to Azure Data Lake Gen2.

Accepts OTLP JSON payloads from any OpenTelemetry exporter, parses and
normalizes the data, and writes partitioned Parquet files to ADLS Gen2.
"""

import asyncio
import gzip
import json
import logging
import os
import secrets
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

# Configure logging EARLY - before any module imports that use logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from starlette.middleware.gzip import GZipMiddleware  # noqa: E402

from src.datalake_writer import DataLakeConfig, DataLakeWriter  # noqa: E402
from src.ingestion_router import IngestionConfig, IngestionRouter  # noqa: E402
from src.tracing import setup_tracing, shutdown_tracing  # noqa: E402

logger = logging.getLogger(__name__)


async def verify_bearer_token(authorization: str | None = Header(default=None)) -> None:
    """Reject ingest requests without a matching Authorization: Bearer <token>.

    Compatible with OTLP-source authentication patterns that send a standard
    `Authorization` header (RFC 6750). Falls closed: if INGEST_BEARER_TOKEN is
    unset on the server, every request is refused with 503 so a misconfigured
    deploy does not silently accept data.
    """
    expected = os.getenv("INGEST_BEARER_TOKEN", "")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Ingest auth not configured (set INGEST_BEARER_TOKEN)",
        )
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header (expected: Bearer <token>)",
        )
    token = authorization[len("Bearer ") :].strip()
    if not token or not secrets.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="Invalid bearer token")


# Global state
datalake_writer: DataLakeWriter | None = None
ingestion_router: IngestionRouter | None = None
background_tasks: dict[str, asyncio.Task] = {}
shutdown_event = asyncio.Event()

# In-memory metrics counters for /metrics endpoint
ingestion_metrics: dict[str, Any] = {
    "requests_total": 0,
    "requests_success": 0,
    "requests_error": 0,
    "metrics_ingested": 0,
    "datalake_flushes": 0,
    "datalake_records_written": 0,
    "started_at": datetime.now().isoformat(),
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle management for the application."""
    logger.info("Starting HttpIngest...")

    global datalake_writer, ingestion_router, background_tasks

    # Initialize Data Lake writer (primary storage)
    try:
        datalake_config = DataLakeConfig.from_env()
        datalake_writer = DataLakeWriter(datalake_config)
        logger.info(f"Data Lake writer initialized (account: {datalake_config.account_name})")
    except Exception as e:
        logger.error(f"Failed to initialize Data Lake writer: {e}")
        datalake_writer = None

    # Initialize ingestion router
    ingestion_config = IngestionConfig(
        write_to_datalake=datalake_writer is not None,
    )
    ingestion_router = IngestionRouter(
        datalake_writer=datalake_writer,
        config=ingestion_config,
    )
    logger.info(f"Ingestion router initialized (datalake={ingestion_config.write_to_datalake})")

    # Start background tasks
    if datalake_writer:
        background_tasks["datalake_flush"] = asyncio.create_task(datalake_flush_loop())
        logger.info(f"Background tasks started: {list(background_tasks.keys())}")

    yield

    # Cleanup on shutdown
    logger.info("Shutting down HttpIngest...")
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

    # Shutdown tracing (flush pending spans)
    shutdown_tracing()

    logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="HttpIngest",
    description="OTLP metrics ingestion to Azure Data Lake Gen2",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# Response compression
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Append standard security headers to every response.

    Cheap defense-in-depth against common scanner findings (clickjacking,
    MIME sniffing, downgrade attacks, referrer leakage).
    """
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
    return response


# Initialize tracing after app creation (before routes are registered)
setup_tracing(app)


# ============================================================================
# BACKGROUND TASKS
# ============================================================================


async def datalake_flush_loop():
    """Background task to periodically flush Data Lake buffer."""
    logger.info("Starting Data Lake flush loop...")

    flush_interval = int(os.getenv("DATALAKE_FLUSH_INTERVAL_SECONDS", "600"))

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


# ============================================================================
# HEALTH & INGESTION ENDPOINTS
# ============================================================================


@app.get("/health")
async def health_root():
    """Root health check for Azure Container Apps probes."""
    if datalake_writer is None:
        return JSONResponse(
            content={
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "version": app.version,
                "error": "Data Lake writer not initialized",
            },
            status_code=503,
        )
    return JSONResponse(
        content={
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": app.version,
        }
    )


@app.get("/api/health", dependencies=[Depends(verify_bearer_token)])
async def health_check():
    """Detailed health check with component status.

    Bearer-gated because the response exposes buffer sizes, task names, and
    component configuration that should not be public-readable.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": app.version,
        "components": {},
    }

    # Check Data Lake writer (primary storage)
    if datalake_writer:
        health_status["components"]["datalake"] = {
            "status": "healthy",
            "buffer": datalake_writer.get_buffer_stats(),
        }
    else:
        health_status["components"]["datalake"] = {"status": "not initialized"}
        health_status["status"] = "degraded"

    # Check ingestion router
    if ingestion_router:
        router_status = await ingestion_router.get_status()
        health_status["components"]["ingestion_router"] = router_status
    else:
        health_status["components"]["ingestion_router"] = "not initialized"
        health_status["status"] = "degraded"

    # Check background tasks
    running_tasks = sum(1 for t in background_tasks.values() if not t.done())
    health_status["components"]["background_tasks"] = {
        "running": running_tasks,
        "total": len(background_tasks),
        "tasks": list(background_tasks.keys()),
    }

    status_code = 200 if health_status["status"] == "healthy" else 503

    return JSONResponse(content=health_status, status_code=status_code)


@app.post("/api/HttpIngest", dependencies=[Depends(verify_bearer_token)])
async def http_ingest(request: Request):
    """OTLP ingestion endpoint.

    Accepts OTLP JSON payloads and writes them to Azure Data Lake Gen2 as
    partitioned Parquet files for downstream analytics and ML training.
    """
    try:
        content_type = request.headers.get("content-type", "application/json")

        # Pre-read body size cap. OTLP batches are typically <1MB; cap at 10MB
        # to bound memory + reject gzip bombs before decompression. Content-
        # Length header is set by the OTLP exporter; absence is suspicious.
        max_body_bytes = int(os.getenv("INGEST_MAX_BODY_BYTES", str(10 * 1024 * 1024)))
        max_decompressed_bytes = int(
            os.getenv("INGEST_MAX_DECOMPRESSED_BYTES", str(100 * 1024 * 1024))
        )
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > max_body_bytes:
            return JSONResponse(
                content={"error": "payload too large"},
                status_code=413,
            )

        body = await request.body()
        if len(body) > max_body_bytes:
            return JSONResponse(
                content={"error": "payload too large"},
                status_code=413,
            )

        # Handle gzip compression with decompressed-size cap to prevent gzip bombs.
        if "gzip" in content_type or request.headers.get("content-encoding") == "gzip":
            import io

            try:
                with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
                    body = gz.read(max_decompressed_bytes + 1)
            except (gzip.BadGzipFile, OSError):
                return JSONResponse(
                    content={"error": "invalid gzip payload"},
                    status_code=400,
                )
            if len(body) > max_decompressed_bytes:
                return JSONResponse(
                    content={"error": "decompressed payload too large"},
                    status_code=413,
                )

        # Parse JSON
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return JSONResponse(content={"error": "invalid JSON"}, status_code=400)

        # Validate OTLP structure
        if "resourceMetrics" not in payload:
            return JSONResponse(
                content={"error": "Missing resourceMetrics in OTLP payload"},
                status_code=400,
            )

        if not ingestion_router:
            logger.error("Ingestion router not initialized")
            return JSONResponse(
                content={"error": "Ingestion service not available"}, status_code=503
            )

        # Route to Data Lake
        ingestion_metrics["requests_total"] += 1
        stats = await ingestion_router.ingest(payload)

        if stats.errors and stats.datalake_written == 0:
            ingestion_metrics["requests_error"] += 1
            return JSONResponse(
                content={
                    "status": "error",
                    "errors": stats.errors,
                    "timestamp": datetime.now().isoformat(),
                },
                status_code=500,
            )

        ingestion_metrics["requests_success"] += 1
        ingestion_metrics["metrics_ingested"] += stats.datalake_written

        return JSONResponse(
            content={
                "status": "success",
                "stats": stats.to_dict(),
                "timestamp": datetime.now().isoformat(),
            },
            status_code=200,
        )

    except Exception as e:
        ingestion_metrics["requests_total"] += 1
        ingestion_metrics["requests_error"] += 1
        logger.error(f"Ingestion error: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/metrics", dependencies=[Depends(verify_bearer_token)])
async def prometheus_metrics():
    """Prometheus metrics endpoint using in-memory counters.

    Bearer-gated to avoid leaking ingestion stats / model names / operational
    surface to anonymous scanners. Same INGEST_BEARER_TOKEN as /api/HttpIngest.
    """
    buffer_stats = datalake_writer.get_buffer_stats() if datalake_writer else {}

    lines = [
        "# HELP httpingest_requests_total Total HTTP ingest requests received",
        "# TYPE httpingest_requests_total counter",
        f"httpingest_requests_total {ingestion_metrics['requests_total']}",
        "# HELP httpingest_requests_success_total Successful ingest requests",
        "# TYPE httpingest_requests_success_total counter",
        f"httpingest_requests_success_total {ingestion_metrics['requests_success']}",
        "# HELP httpingest_requests_error_total Failed ingest requests",
        "# TYPE httpingest_requests_error_total counter",
        f"httpingest_requests_error_total {ingestion_metrics['requests_error']}",
        "# HELP httpingest_metrics_ingested_total Total individual metrics ingested",
        "# TYPE httpingest_metrics_ingested_total counter",
        f"httpingest_metrics_ingested_total {ingestion_metrics['metrics_ingested']}",
        "# HELP httpingest_datalake_flushes_total Data Lake flush operations",
        "# TYPE httpingest_datalake_flushes_total counter",
        f"httpingest_datalake_flushes_total {ingestion_metrics['datalake_flushes']}",
        "# HELP httpingest_datalake_records_written_total Records written to Data Lake",
        "# TYPE httpingest_datalake_records_written_total counter",
        (
            "httpingest_datalake_records_written_total "
            f"{ingestion_metrics['datalake_records_written']}"
        ),
        "# HELP httpingest_datalake_buffer_size Current Data Lake buffer size",
        "# TYPE httpingest_datalake_buffer_size gauge",
        f"httpingest_datalake_buffer_size {buffer_stats.get('metric_data_buffered', 0)}",
        "# HELP httpingest_info Application info",
        "# TYPE httpingest_info gauge",
        f'httpingest_info{{version="{app.version}"}} 1',
        "",
    ]
    return Response(content="\n".join(lines), media_type="text/plain; version=0.0.4")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
