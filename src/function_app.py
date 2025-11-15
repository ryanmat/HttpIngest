import azure.functions as func
import logging
import json
import gzip
import os
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone
import subprocess
import asyncio
from typing import Optional
from dataclasses import dataclass, asdict

from src.data_processor import DataProcessor, BatchProcessingStats

app = func.FunctionApp()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Circuit Breaker for Database Operations
@dataclass
class CircuitBreakerState:
    """Circuit breaker state for database failure handling."""
    failures: int = 0
    last_failure_time: Optional[datetime] = None
    is_open: bool = False
    success_count: int = 0


class CircuitBreaker:
    """
    Circuit breaker pattern for database operations.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failures exceeded threshold, requests blocked
    - HALF_OPEN: Testing if service recovered

    Configuration:
    - failure_threshold: Number of failures before opening circuit
    - recovery_timeout: Seconds to wait before attempting recovery
    - success_threshold: Successes needed to close circuit in half-open state
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, success_threshold: int = 2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.state = CircuitBreakerState()

    def record_success(self):
        """Record a successful operation."""
        if self.state.is_open:
            self.state.success_count += 1
            logger.info(f"Circuit breaker: Success recorded ({self.state.success_count}/{self.success_threshold})")

            if self.state.success_count >= self.success_threshold:
                # Close the circuit
                logger.info("Circuit breaker: Closing circuit after successful recovery")
                self.state.failures = 0
                self.state.is_open = False
                self.state.success_count = 0
                self.state.last_failure_time = None
        else:
            # Reset failure count on success
            self.state.failures = 0
            self.state.success_count = 0

    def record_failure(self):
        """Record a failed operation."""
        self.state.failures += 1
        self.state.last_failure_time = datetime.now(timezone.utc)
        self.state.success_count = 0

        logger.warning(f"Circuit breaker: Failure recorded ({self.state.failures}/{self.failure_threshold})")

        if self.state.failures >= self.failure_threshold:
            logger.error("Circuit breaker: Opening circuit due to repeated failures")
            self.state.is_open = True

    def can_attempt(self) -> bool:
        """Check if operation should be attempted."""
        if not self.state.is_open:
            return True

        # Check if recovery timeout has passed
        if self.state.last_failure_time:
            time_since_failure = (datetime.now(timezone.utc) - self.state.last_failure_time).total_seconds()
            if time_since_failure >= self.recovery_timeout:
                logger.info("Circuit breaker: Attempting recovery (half-open state)")
                return True

        logger.warning("Circuit breaker: Circuit is open, blocking operation")
        return False

    def get_state(self) -> dict:
        """Get current circuit breaker state."""
        return {
            "is_open": self.state.is_open,
            "failures": self.state.failures,
            "success_count": self.state.success_count,
            "last_failure": self.state.last_failure_time.isoformat() if self.state.last_failure_time else None
        }


# Global circuit breaker instance
db_circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60, success_threshold=2)

def get_db_connection():
    """Get database connection with token refresh"""
    host = os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')
    database = os.environ.get('PGDATABASE', 'postgres')
    user = os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')
    port = os.environ.get('PGPORT', '5432')
    password = os.environ.get('PGPASSWORD', '')
    
    # If no password or connection fails, try to get fresh token
    if not password:
        try:
            logger.info("Getting fresh Azure AD token")
            result = subprocess.run(
                ["az", "account", "get-access-token", 
                 "--resource", "https://ossrdbms-aad.database.windows.net", 
                 "--query", "accessToken", "--output", "tsv"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                password = result.stdout.strip()
                logger.info("Got fresh token successfully")
            else:
                raise Exception(f"Failed to get token: {result.stderr}")
        except Exception as e:
            logger.error(f"Token acquisition failed: {e}")
            raise
    
    conn_str = f"host={host} port={port} dbname={database} user={user} password={password} sslmode=require"
    return psycopg2.connect(conn_str)

@app.function_name(name="health")
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Health check endpoint called')

    db_status = "unknown"
    processing_stats = {}

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get total lm_metrics count
                cur.execute("SELECT COUNT(*) FROM lm_metrics")
                total_records = cur.fetchone()[0]

                # Get processing status breakdown
                cur.execute("""
                    SELECT
                        status,
                        COUNT(*) as count
                    FROM processing_status
                    GROUP BY status
                """)
                status_counts = {row[0]: row[1] for row in cur.fetchall()}

                # Get unprocessed count
                cur.execute("""
                    SELECT COUNT(*)
                    FROM lm_metrics lm
                    LEFT JOIN processing_status ps ON lm.id = ps.lm_metrics_id
                    WHERE ps.id IS NULL
                """)
                unprocessed = cur.fetchone()[0]

                # Get total processed data
                cur.execute("SELECT COUNT(*) FROM metric_data")
                total_data_points = cur.fetchone()[0]

                db_status = f"connected - {total_records} records"
                processing_stats = {
                    "total_records": total_records,
                    "unprocessed": unprocessed,
                    "pending": status_counts.get("pending", 0),
                    "processing": status_counts.get("processing", 0),
                    "success": status_counts.get("success", 0),
                    "failed": status_counts.get("failed", 0),
                    "total_data_points": total_data_points
                }

    except Exception as e:
        logger.error(f"Health check DB error: {e}")
        db_status = f"error: {str(e)[:50]}"

    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "service": "LogicMonitor HTTP Ingest",
            "version": "10.0",
            "database": db_status,
            "processing": processing_stats,
            "circuit_breaker": db_circuit_breaker.get_state(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )

@app.function_name(name="HttpIngest")
@app.route(route="HttpIngest", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('HTTP Ingest triggered')
    
    try:
        # Get request body
        body = req.get_body()
        logger.info(f"Received {len(body)} bytes")
        
        # Check for gzip
        content_encoding = req.headers.get('content-encoding', '').lower()
        if content_encoding == 'gzip' or (len(body) > 2 and body[:2] == b'\x1f\x8b'):
            try:
                body = gzip.decompress(body)
                logger.info(f"Decompressed to {len(body)} bytes")
            except Exception as e:
                logger.warning(f"Decompression failed: {e}")
        
        # Parse JSON
        try:
            data = json.loads(body)
            logger.info(f"Parsed JSON successfully")
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            logger.error(f"First 200 chars: {body[:200]}")
            return func.HttpResponse(
                json.dumps({"error": f"Invalid JSON: {str(e)}"}),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        # Store in database with retry
        retries = 2
        insert_id = None
        last_error = None
        
        for attempt in range(retries):
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        # Create table if needed
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS lm_metrics (
                                id SERIAL PRIMARY KEY,
                                payload JSONB NOT NULL,
                                ingested_at TIMESTAMPTZ DEFAULT NOW()
                            )
                        """)
                        
                        # Insert data
                        cur.execute(
                            "INSERT INTO lm_metrics (payload) VALUES (%s) RETURNING id",
                            (Json(data),)
                        )
                        insert_id = cur.fetchone()[0]
                        conn.commit()
                        logger.info(f"Inserted record {insert_id}")
                        break
            except Exception as e:
                last_error = e
                logger.error(f"DB attempt {attempt+1} failed: {e}")
                if attempt == 0:
                    # Try to refresh token on first failure
                    os.environ['PGPASSWORD'] = ''  # Force token refresh
        
        if insert_id:
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "id": insert_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }),
                status_code=200,
                headers={"Content-Type": "application/json"}
            )
        else:
            raise last_error
            
    except Exception as e:
        logger.error(f"Ingestion error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )

@app.function_name(name="root")
@app.route(route="", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def root(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({
            "service": "LogicMonitor HTTP Ingest",
            "version": "10.0",
            "endpoints": [
                "/api/health",
                "/api/HttpIngest",
                "/api/process",
                "/api/metrics",
                "/api/resources",
                "/api/timeseries",
                "/api/aggregates"
            ],
            "documentation": "/api/docs"
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )


@app.function_name(name="process")
@app.route(route="process", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def process_metrics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Process pending lm_metrics records into normalized tables.

    Query parameters:
    - batch_size: Number of records to process (default: 100, max: 1000)
    - continue_on_error: Continue processing if some records fail (default: true)

    Returns:
    - Processing statistics including success/failure counts
    """
    logger.info('Process metrics endpoint called')

    # Check circuit breaker
    if not db_circuit_breaker.can_attempt():
        return func.HttpResponse(
            json.dumps({
                "error": "Service temporarily unavailable due to database failures",
                "circuit_breaker": db_circuit_breaker.get_state(),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=503,
            headers={"Content-Type": "application/json"}
        )

    try:
        # Parse query parameters
        batch_size = int(req.params.get('batch_size', '100'))
        continue_on_error = req.params.get('continue_on_error', 'true').lower() == 'true'

        # Validate batch size
        if batch_size < 1:
            return func.HttpResponse(
                json.dumps({"error": "batch_size must be at least 1"}),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        if batch_size > 1000:
            logger.warning(f"Requested batch_size {batch_size} exceeds maximum, using 1000")
            batch_size = 1000

        logger.info(f"Processing batch: size={batch_size}, continue_on_error={continue_on_error}")

        # Run async processing
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                process_batch_async(batch_size, continue_on_error)
            )
        finally:
            loop.close()

        # Record success with circuit breaker
        db_circuit_breaker.record_success()

        # Build response
        response_data = {
            "status": "completed",
            "stats": asdict(result),
            "circuit_breaker": db_circuit_breaker.get_state(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        logger.error(f"Processing error: {str(e)}", exc_info=True)

        # Record failure with circuit breaker
        db_circuit_breaker.record_failure()

        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "circuit_breaker": db_circuit_breaker.get_state(),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


# ============================================================================
# Query Endpoints - Metrics, Resources, Time-Series, Aggregates
# ============================================================================

def parse_pagination_params(req: func.HttpRequest) -> dict:
    """Parse common pagination parameters from request."""
    try:
        page = max(1, int(req.params.get('page', '1')))
        page_size = min(1000, max(1, int(req.params.get('page_size', '100'))))
        offset = (page - 1) * page_size
        return {
            "page": page,
            "page_size": page_size,
            "offset": offset,
            "limit": page_size
        }
    except ValueError:
        return {"page": 1, "page_size": 100, "offset": 0, "limit": 100}


def build_paginated_response(data: list, total_count: int, page: int, page_size: int) -> dict:
    """Build paginated response with metadata."""
    total_pages = (total_count + page_size - 1) // page_size if page_size > 0 else 1

    return {
        "data": data,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total_count,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.function_name(name="metrics")
@app.route(route="metrics", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_metrics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get available metrics with optional filtering.

    Query parameters:
    - datasource: Filter by datasource name
    - metric_type: Filter by metric type (gauge, sum, histogram, etc.)
    - search: Search in metric name or description
    - page: Page number (default: 1)
    - page_size: Items per page (default: 100, max: 1000)
    - sort: Sort field (name, type, created_at) (default: name)
    - order: Sort order (asc, desc) (default: asc)

    Returns:
    - List of metrics with pagination metadata
    """
    logger.info('Metrics query endpoint called')

    try:
        # Parse parameters
        pagination = parse_pagination_params(req)
        datasource_filter = req.params.get('datasource')
        metric_type_filter = req.params.get('metric_type')
        search_term = req.params.get('search')
        sort_field = req.params.get('sort', 'name')
        sort_order = req.params.get('order', 'asc').upper()

        # Validate sort parameters
        valid_sort_fields = ['name', 'type', 'created_at', 'datasource_name']
        if sort_field not in valid_sort_fields:
            sort_field = 'name'

        if sort_order not in ['ASC', 'DESC']:
            sort_order = 'ASC'

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build WHERE clause
                where_clauses = []
                params = []

                if datasource_filter:
                    where_clauses.append("ds.name ILIKE %s")
                    params.append(f"%{datasource_filter}%")

                if metric_type_filter:
                    where_clauses.append("mdef.metric_type = %s")
                    params.append(metric_type_filter)

                if search_term:
                    where_clauses.append("(mdef.name ILIKE %s OR mdef.description ILIKE %s)")
                    params.extend([f"%{search_term}%", f"%{search_term}%"])

                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                # Get total count
                count_query = f"""
                    SELECT COUNT(DISTINCT mdef.id)
                    FROM metric_definitions mdef
                    JOIN datasources ds ON mdef.datasource_id = ds.id
                    {where_sql}
                """
                cur.execute(count_query, params)
                total_count = cur.fetchone()[0]

                # Get metrics with pagination
                query = f"""
                    SELECT
                        mdef.id,
                        mdef.name,
                        mdef.unit,
                        mdef.metric_type as type,
                        mdef.description,
                        ds.name as datasource_name,
                        ds.version as datasource_version,
                        COUNT(DISTINCT md.resource_id) as resource_count,
                        COUNT(md.id) as data_point_count,
                        MAX(md.timestamp) as last_data_point,
                        mdef.created_at
                    FROM metric_definitions mdef
                    JOIN datasources ds ON mdef.datasource_id = ds.id
                    LEFT JOIN metric_data md ON mdef.id = md.metric_definition_id
                    {where_sql}
                    GROUP BY mdef.id, mdef.name, mdef.unit, mdef.metric_type, mdef.description,
                             ds.name, ds.version, mdef.created_at
                    ORDER BY {sort_field} {sort_order}
                    LIMIT %s OFFSET %s
                """

                params.extend([pagination['limit'], pagination['offset']])
                cur.execute(query, params)

                metrics = []
                for row in cur.fetchall():
                    metrics.append({
                        "id": row[0],
                        "name": row[1],
                        "unit": row[2],
                        "type": row[3],
                        "description": row[4],
                        "datasource": {
                            "name": row[5],
                            "version": row[6]
                        },
                        "stats": {
                            "resource_count": row[7],
                            "data_point_count": row[8],
                            "last_data_point": row[9].isoformat() if row[9] else None
                        },
                        "created_at": row[10].isoformat()
                    })

                response_data = build_paginated_response(
                    metrics, total_count, pagination['page'], pagination['page_size']
                )

                return func.HttpResponse(
                    json.dumps(response_data),
                    status_code=200,
                    headers={"Content-Type": "application/json"}
                )

    except Exception as e:
        logger.error(f"Metrics query error: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.function_name(name="resources")
@app.route(route="resources", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_resources(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get resources with metric counts and activity.

    Query parameters:
    - service_name: Filter by service.name attribute
    - host_name: Filter by host.name attribute
    - min_metrics: Minimum number of metrics (default: 0)
    - active_since: ISO timestamp - only resources with data since this time
    - page: Page number (default: 1)
    - page_size: Items per page (default: 100, max: 1000)
    - sort: Sort field (created_at, metric_count, last_data) (default: created_at)
    - order: Sort order (asc, desc) (default: desc)

    Returns:
    - List of resources with metric counts and pagination metadata
    """
    logger.info('Resources query endpoint called')

    try:
        # Parse parameters
        pagination = parse_pagination_params(req)
        service_name = req.params.get('service_name')
        host_name = req.params.get('host_name')
        min_metrics = int(req.params.get('min_metrics', '0'))
        active_since = req.params.get('active_since')
        sort_field = req.params.get('sort', 'created_at')
        sort_order = req.params.get('order', 'desc').upper()

        # Validate sort parameters
        valid_sort_fields = ['created_at', 'metric_count', 'last_data', 'data_point_count']
        if sort_field not in valid_sort_fields:
            sort_field = 'created_at'

        if sort_order not in ['ASC', 'DESC']:
            sort_order = 'DESC'

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build WHERE clause
                where_clauses = []
                params = []

                if service_name:
                    where_clauses.append("r.attributes->>'service.name' ILIKE %s")
                    params.append(f"%{service_name}%")

                if host_name:
                    where_clauses.append("r.attributes->>'host.name' ILIKE %s")
                    params.append(f"%{host_name}%")

                if active_since:
                    where_clauses.append("MAX(md.timestamp) >= %s")
                    params.append(active_since)

                having_clause = f"HAVING COUNT(DISTINCT md.metric_definition_id) >= {min_metrics}" if min_metrics > 0 else ""
                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                # Get total count
                count_query = f"""
                    SELECT COUNT(*) FROM (
                        SELECT r.id
                        FROM resources r
                        LEFT JOIN metric_data md ON r.id = md.resource_id
                        {where_sql}
                        GROUP BY r.id
                        {having_clause}
                    ) subq
                """
                cur.execute(count_query, params)
                total_count = cur.fetchone()[0]

                # Get resources with pagination
                query = f"""
                    SELECT
                        r.id,
                        r.resource_hash,
                        r.attributes,
                        r.created_at,
                        r.updated_at,
                        COUNT(DISTINCT md.metric_definition_id) as metric_count,
                        COUNT(md.id) as data_point_count,
                        MAX(md.timestamp) as last_data_point,
                        MIN(md.timestamp) as first_data_point,
                        ARRAY_AGG(DISTINCT ds.name) FILTER (WHERE ds.name IS NOT NULL) as datasources
                    FROM resources r
                    LEFT JOIN metric_data md ON r.id = md.resource_id
                    LEFT JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
                    LEFT JOIN datasources ds ON mdef.datasource_id = ds.id
                    {where_sql}
                    GROUP BY r.id, r.resource_hash, r.attributes, r.created_at, r.updated_at
                    {having_clause}
                    ORDER BY {sort_field} {sort_order}
                    LIMIT %s OFFSET %s
                """

                params.extend([pagination['limit'], pagination['offset']])
                cur.execute(query, params)

                resources = []
                for row in cur.fetchall():
                    resources.append({
                        "id": row[0],
                        "resource_hash": row[1],
                        "attributes": row[2],
                        "created_at": row[3].isoformat(),
                        "updated_at": row[4].isoformat(),
                        "metrics": {
                            "unique_metrics": row[5],
                            "total_data_points": row[6],
                            "last_data_point": row[7].isoformat() if row[7] else None,
                            "first_data_point": row[8].isoformat() if row[8] else None,
                            "datasources": row[9] if row[9] else []
                        }
                    })

                response_data = build_paginated_response(
                    resources, total_count, pagination['page'], pagination['page_size']
                )

                return func.HttpResponse(
                    json.dumps(response_data),
                    status_code=200,
                    headers={"Content-Type": "application/json"}
                )

    except Exception as e:
        logger.error(f"Resources query error: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.function_name(name="timeseries")
@app.route(route="timeseries", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_timeseries(req: func.HttpRequest) -> func.HttpResponse:
    """
    Retrieve time-series data for specific metrics and resources.

    Query parameters (required):
    - metric_name: Metric name to query
    - start_time: Start timestamp (ISO format)
    - end_time: End timestamp (ISO format)

    Query parameters (optional):
    - resource_hash: Filter by specific resource
    - datasource: Filter by datasource name
    - page: Page number (default: 1)
    - page_size: Items per page (default: 1000, max: 10000)
    - include_attributes: Include metric attributes (default: false)

    Returns:
    - Time-series data points with pagination metadata
    """
    logger.info('Time-series query endpoint called')

    try:
        # Validate required parameters
        metric_name = req.params.get('metric_name')
        start_time = req.params.get('start_time')
        end_time = req.params.get('end_time')

        if not metric_name or not start_time or not end_time:
            return func.HttpResponse(
                json.dumps({
                    "error": "Missing required parameters: metric_name, start_time, end_time",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # Parse optional parameters
        pagination = parse_pagination_params(req)
        # Allow larger page sizes for time-series data
        pagination['page_size'] = min(10000, pagination['page_size'])
        pagination['limit'] = pagination['page_size']

        resource_hash = req.params.get('resource_hash')
        datasource_filter = req.params.get('datasource')
        include_attributes = req.params.get('include_attributes', 'false').lower() == 'true'

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build WHERE clause
                where_clauses = ["mdef.name = %s", "md.timestamp >= %s", "md.timestamp <= %s"]
                params = [metric_name, start_time, end_time]

                if resource_hash:
                    where_clauses.append("r.resource_hash = %s")
                    params.append(resource_hash)

                if datasource_filter:
                    where_clauses.append("ds.name = %s")
                    params.append(datasource_filter)

                where_sql = " AND ".join(where_clauses)

                # Get total count
                count_query = f"""
                    SELECT COUNT(*)
                    FROM metric_data md
                    JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
                    JOIN datasources ds ON mdef.datasource_id = ds.id
                    JOIN resources r ON md.resource_id = r.id
                    WHERE {where_sql}
                """
                cur.execute(count_query, params)
                total_count = cur.fetchone()[0]

                # Get time-series data
                attributes_field = ", md.attributes" if include_attributes else ""

                query = f"""
                    SELECT
                        md.timestamp,
                        md.value_double,
                        md.value_int,
                        r.resource_hash,
                        r.attributes->>'service.name' as service_name,
                        r.attributes->>'host.name' as host_name,
                        mdef.unit,
                        ds.name as datasource_name
                        {attributes_field}
                    FROM metric_data md
                    JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
                    JOIN datasources ds ON mdef.datasource_id = ds.id
                    JOIN resources r ON md.resource_id = r.id
                    WHERE {where_sql}
                    ORDER BY md.timestamp ASC
                    LIMIT %s OFFSET %s
                """

                params.extend([pagination['limit'], pagination['offset']])
                cur.execute(query, params)

                datapoints = []
                for row in cur.fetchall():
                    datapoint = {
                        "timestamp": row[0].isoformat(),
                        "value": row[1] if row[1] is not None else row[2],
                        "resource": {
                            "resource_hash": row[3],
                            "service_name": row[4],
                            "host_name": row[5]
                        },
                        "unit": row[6],
                        "datasource": row[7]
                    }

                    if include_attributes and len(row) > 8:
                        datapoint["attributes"] = row[8]

                    datapoints.append(datapoint)

                response_data = build_paginated_response(
                    datapoints, total_count, pagination['page'], pagination['page_size']
                )

                # Add query info
                response_data["query"] = {
                    "metric_name": metric_name,
                    "start_time": start_time,
                    "end_time": end_time,
                    "resource_hash": resource_hash,
                    "datasource": datasource_filter
                }

                return func.HttpResponse(
                    json.dumps(response_data),
                    status_code=200,
                    headers={"Content-Type": "application/json"}
                )

    except Exception as e:
        logger.error(f"Time-series query error: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.function_name(name="aggregates")
@app.route(route="aggregates", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_aggregates(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get aggregated time-series data by time bucket.

    Query parameters (required):
    - metric_name: Metric name to query
    - start_time: Start timestamp (ISO format)
    - end_time: End timestamp (ISO format)
    - bucket: Time bucket size (1m, 5m, 15m, 1h, 6h, 1d, 7d, 30d)

    Query parameters (optional):
    - resource_hash: Filter by specific resource
    - datasource: Filter by datasource name
    - aggregation: Aggregation function (min, max, avg, sum, count) (default: avg)
    - page: Page number (default: 1)
    - page_size: Items per page (default: 1000, max: 10000)

    Returns:
    - Aggregated data points with MIN, MAX, AVG, SUM, COUNT per bucket
    """
    logger.info('Aggregates query endpoint called')

    try:
        # Validate required parameters
        metric_name = req.params.get('metric_name')
        start_time = req.params.get('start_time')
        end_time = req.params.get('end_time')
        bucket = req.params.get('bucket')

        if not metric_name or not start_time or not end_time or not bucket:
            return func.HttpResponse(
                json.dumps({
                    "error": "Missing required parameters: metric_name, start_time, end_time, bucket",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # Map bucket sizes to seconds for epoch-based bucketing
        bucket_map = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '6h': 21600,
            '1d': 86400,
            '7d': 604800,
            '30d': 2592000
        }

        if bucket not in bucket_map:
            return func.HttpResponse(
                json.dumps({
                    "error": f"Invalid bucket size. Valid values: {', '.join(bucket_map.keys())}",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        bucket_seconds = bucket_map[bucket]

        # Parse optional parameters
        pagination = parse_pagination_params(req)
        pagination['page_size'] = min(10000, pagination['page_size'])
        pagination['limit'] = pagination['page_size']

        resource_hash = req.params.get('resource_hash')
        datasource_filter = req.params.get('datasource')

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build WHERE clause
                where_clauses = ["mdef.name = %s", "md.timestamp >= %s", "md.timestamp <= %s"]
                where_params = [metric_name, start_time, end_time]

                if resource_hash:
                    where_clauses.append("r.resource_hash = %s")
                    where_params.append(resource_hash)

                if datasource_filter:
                    where_clauses.append("ds.name = %s")
                    where_params.append(datasource_filter)

                where_sql = " AND ".join(where_clauses)

                # Get aggregated data using epoch-based bucketing
                query = f"""
                    SELECT
                        time_bucket,
                        COUNT(*) as data_point_count,
                        MIN(value) as min_value,
                        MAX(value) as max_value,
                        AVG(value) as avg_value,
                        SUM(value) as sum_value,
                        STDDEV(value) as stddev_value,
                        COUNT(DISTINCT resource_id) as resource_count
                    FROM (
                        SELECT
                            to_timestamp(floor(extract(epoch from md.timestamp) / %s) * %s) as time_bucket,
                            COALESCE(md.value_double, md.value_int::float) as value,
                            md.resource_id
                        FROM metric_data md
                        JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
                        JOIN datasources ds ON mdef.datasource_id = ds.id
                        JOIN resources r ON md.resource_id = r.id
                        WHERE {where_sql}
                    ) data
                    GROUP BY time_bucket
                    ORDER BY time_bucket ASC
                    LIMIT %s OFFSET %s
                """

                # Build params in the order they appear in the query
                params = [bucket_seconds, bucket_seconds] + where_params + [pagination['limit'], pagination['offset']]
                cur.execute(query, params)

                aggregates = []
                for row in cur.fetchall():
                    aggregates.append({
                        "time_bucket": row[0].isoformat(),
                        "stats": {
                            "count": row[1],
                            "min": float(row[2]) if row[2] is not None else None,
                            "max": float(row[3]) if row[3] is not None else None,
                            "avg": float(row[4]) if row[4] is not None else None,
                            "sum": float(row[5]) if row[5] is not None else None,
                            "stddev": float(row[6]) if row[6] is not None else None,
                            "resource_count": row[7]
                        }
                    })

                # Get total bucket count for pagination
                count_query = f"""
                    SELECT COUNT(DISTINCT to_timestamp(floor(extract(epoch from md.timestamp) / %s) * %s))
                    FROM metric_data md
                    JOIN metric_definitions mdef ON md.metric_definition_id = mdef.id
                    JOIN datasources ds ON mdef.datasource_id = ds.id
                    JOIN resources r ON md.resource_id = r.id
                    WHERE {where_sql}
                """
                # Use first part of params (bucket_seconds twice + where_params)
                cur.execute(count_query, params[:-2])  # Exclude limit/offset
                total_count = cur.fetchone()[0]

                response_data = build_paginated_response(
                    aggregates, total_count, pagination['page'], pagination['page_size']
                )

                # Add query info
                response_data["query"] = {
                    "metric_name": metric_name,
                    "start_time": start_time,
                    "end_time": end_time,
                    "bucket": bucket,
                    "bucket_seconds": bucket_seconds,
                    "resource_hash": resource_hash,
                    "datasource": datasource_filter
                }

                return func.HttpResponse(
                    json.dumps(response_data),
                    status_code=200,
                    headers={"Content-Type": "application/json"}
                )

    except Exception as e:
        logger.error(f"Aggregates query error: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.function_name(name="docs")
@app.route(route="docs", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_docs(req: func.HttpRequest) -> func.HttpResponse:
    """
    OpenAPI 3.0 documentation for the API.

    Returns OpenAPI specification in JSON format.
    """
    openapi_spec = {
        "openapi": "3.0.0",
        "info": {
            "title": "LogicMonitor HTTP Ingest API",
            "version": "10.0",
            "description": "API for ingesting and querying LogicMonitor metrics data",
            "contact": {
                "name": "API Support",
                "email": "ryan.matuszewski@logicmonitor.com"
            }
        },
        "servers": [
            {
                "url": "/api",
                "description": "API base path"
            }
        ],
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check endpoint",
                    "description": "Returns service health status and processing statistics",
                    "responses": {
                        "200": {
                            "description": "Service is healthy",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {"type": "string"},
                                            "service": {"type": "string"},
                                            "version": {"type": "string"},
                                            "database": {"type": "string"},
                                            "processing": {"type": "object"},
                                            "circuit_breaker": {"type": "object"},
                                            "timestamp": {"type": "string", "format": "date-time"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/metrics": {
                "get": {
                    "summary": "List available metrics",
                    "description": "Get a paginated list of metrics with optional filtering",
                    "parameters": [
                        {
                            "name": "datasource",
                            "in": "query",
                            "description": "Filter by datasource name",
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "metric_type",
                            "in": "query",
                            "description": "Filter by metric type",
                            "schema": {"type": "string", "enum": ["gauge", "sum", "histogram"]}
                        },
                        {
                            "name": "search",
                            "in": "query",
                            "description": "Search in metric name or description",
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "page",
                            "in": "query",
                            "description": "Page number",
                            "schema": {"type": "integer", "default": 1}
                        },
                        {
                            "name": "page_size",
                            "in": "query",
                            "description": "Items per page (max 1000)",
                            "schema": {"type": "integer", "default": 100}
                        },
                        {
                            "name": "sort",
                            "in": "query",
                            "description": "Sort field",
                            "schema": {"type": "string", "enum": ["name", "type", "created_at", "datasource_name"], "default": "name"}
                        },
                        {
                            "name": "order",
                            "in": "query",
                            "description": "Sort order",
                            "schema": {"type": "string", "enum": ["asc", "desc"], "default": "asc"}
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of metrics with pagination",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/Metric"}
                                            },
                                            "pagination": {"$ref": "#/components/schemas/Pagination"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/resources": {
                "get": {
                    "summary": "List resources",
                    "description": "Get a paginated list of resources with metric counts",
                    "parameters": [
                        {
                            "name": "service_name",
                            "in": "query",
                            "description": "Filter by service name",
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "host_name",
                            "in": "query",
                            "description": "Filter by host name",
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "min_metrics",
                            "in": "query",
                            "description": "Minimum number of metrics",
                            "schema": {"type": "integer", "default": 0}
                        },
                        {
                            "name": "page",
                            "in": "query",
                            "schema": {"type": "integer", "default": 1}
                        },
                        {
                            "name": "page_size",
                            "in": "query",
                            "schema": {"type": "integer", "default": 100}
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of resources with pagination"
                        }
                    }
                }
            },
            "/timeseries": {
                "get": {
                    "summary": "Get time-series data",
                    "description": "Retrieve time-series data for specific metrics and resources",
                    "parameters": [
                        {
                            "name": "metric_name",
                            "in": "query",
                            "required": True,
                            "description": "Metric name to query",
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "start_time",
                            "in": "query",
                            "required": True,
                            "description": "Start timestamp (ISO format)",
                            "schema": {"type": "string", "format": "date-time"}
                        },
                        {
                            "name": "end_time",
                            "in": "query",
                            "required": True,
                            "description": "End timestamp (ISO format)",
                            "schema": {"type": "string", "format": "date-time"}
                        },
                        {
                            "name": "resource_hash",
                            "in": "query",
                            "description": "Filter by resource hash",
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "page",
                            "in": "query",
                            "schema": {"type": "integer", "default": 1}
                        },
                        {
                            "name": "page_size",
                            "in": "query",
                            "schema": {"type": "integer", "default": 1000, "maximum": 10000}
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Time-series data points with pagination"
                        },
                        "400": {
                            "description": "Missing required parameters"
                        }
                    }
                }
            },
            "/aggregates": {
                "get": {
                    "summary": "Get aggregated data",
                    "description": "Get aggregated time-series data by time bucket",
                    "parameters": [
                        {
                            "name": "metric_name",
                            "in": "query",
                            "required": True,
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "start_time",
                            "in": "query",
                            "required": True,
                            "schema": {"type": "string", "format": "date-time"}
                        },
                        {
                            "name": "end_time",
                            "in": "query",
                            "required": True,
                            "schema": {"type": "string", "format": "date-time"}
                        },
                        {
                            "name": "bucket",
                            "in": "query",
                            "required": True,
                            "description": "Time bucket size",
                            "schema": {"type": "string", "enum": ["1m", "5m", "15m", "1h", "6h", "1d", "7d", "30d"]}
                        },
                        {
                            "name": "resource_hash",
                            "in": "query",
                            "schema": {"type": "string"}
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Aggregated data with MIN, MAX, AVG, SUM, COUNT per bucket"
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "Metric": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "unit": {"type": "string"},
                        "type": {"type": "string"},
                        "description": {"type": "string"},
                        "datasource": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "version": {"type": "string"}
                            }
                        },
                        "stats": {
                            "type": "object",
                            "properties": {
                                "resource_count": {"type": "integer"},
                                "data_point_count": {"type": "integer"},
                                "last_data_point": {"type": "string", "format": "date-time"}
                            }
                        }
                    }
                },
                "Pagination": {
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer"},
                        "page_size": {"type": "integer"},
                        "total_items": {"type": "integer"},
                        "total_pages": {"type": "integer"},
                        "has_next": {"type": "boolean"},
                        "has_prev": {"type": "boolean"}
                    }
                }
            }
        }
    }

    return func.HttpResponse(
        json.dumps(openapi_spec, indent=2),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )


async def process_batch_async(batch_size: int, continue_on_error: bool) -> BatchProcessingStats:
    """
    Asynchronously process a batch of lm_metrics records.

    Args:
        batch_size: Number of records to process
        continue_on_error: Continue processing if some records fail

    Returns:
        BatchProcessingStats with processing results
    """
    logger.info(f"Starting async batch processing: batch_size={batch_size}")

    # Create database connection (in thread pool to avoid blocking)
    loop = asyncio.get_event_loop()
    conn = await loop.run_in_executor(None, get_db_connection)

    try:
        # Create processor
        processor = DataProcessor(conn)

        # Process batch (run in thread pool since DataProcessor is sync)
        stats = await loop.run_in_executor(
            None,
            lambda: processor.process_batch(limit=batch_size, continue_on_error=continue_on_error)
        )

        logger.info(
            f"Async batch processing complete: {stats.successful}/{stats.total_records} successful, "
            f"{stats.failed} failed, {stats.metric_data_created} data points created"
        )

        return stats

    finally:
        # Clean up connection
        await loop.run_in_executor(None, conn.close)