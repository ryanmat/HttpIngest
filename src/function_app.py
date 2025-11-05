import azure.functions as func
import logging
import json
import gzip
import os
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone
import subprocess

app = func.FunctionApp()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lm_metrics")
                count = cur.fetchone()[0]
                db_status = f"connected - {count} records"
    except Exception as e:
        logger.error(f"Health check DB error: {e}")
        db_status = f"error: {str(e)[:50]}"
    
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "service": "LogicMonitor HTTP Ingest",
            "version": "10.0",
            "database": db_status,
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
            "endpoints": ["/api/health", "/api/HttpIngest"]
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )