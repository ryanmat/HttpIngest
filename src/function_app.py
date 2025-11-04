import azure.functions as func
import logging
import json
import gzip
import os
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone

app = func.FunctionApp()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Simple connection with password from env var"""
    host = os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')
    database = os.environ.get('PGDATABASE', 'postgres')
    user = os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')
    port = os.environ.get('PGPORT', '5432')
    password = os.environ.get('PGPASSWORD', '')
    
    if not password:
        # Try to get token using managed identity
        try:
            import subprocess
            result = subprocess.run(
                ["az", "account", "get-access-token", 
                 "--resource", "https://ossrdbms-aad.database.windows.net", 
                 "--query", "accessToken", "--output", "tsv"],
                capture_output=True, text=True, timeout=5
            )
            password = result.stdout.strip()
        except:
            raise Exception("No password or Azure AD token available")
    
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
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                db_status = f"connected - {version[:30]}..."
    except Exception as e:
        db_status = f"error: {str(e)[:100]}"
    
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "service": "LogicMonitor HTTP Ingest",
            "version": "9.0",
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
        body = req.get_body()
        content_encoding = req.headers.get('content-encoding', '').lower()
        
        if content_encoding == 'gzip' or (len(body) > 2 and body[:2] == b'\x1f\x8b'):
            body = gzip.decompress(body)
            logger.info("Decompressed gzipped content")
        
        data = json.loads(body)
        
        # Store in database
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
                
                cur.execute(
                    "INSERT INTO lm_metrics (payload) VALUES (%s) RETURNING id",
                    (Json(data),)
                )
                insert_id = cur.fetchone()[0]
                conn.commit()
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "id": insert_id,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )

@app.function_name(name="root")
@app.route(route="", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def root(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({
            "service": "LogicMonitor HTTP Ingest",
            "version": "9.0",
            "endpoints": ["/api/health", "/api/HttpIngest"]
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )
