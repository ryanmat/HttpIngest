import azure.functions as func
import logging
import json
import gzip
import os
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Initialize the FunctionApp
app = func.FunctionApp()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection_string():
    """Build PostgreSQL connection string"""
    if os.environ.get('USE_AZURE_AD_AUTH', 'true').lower() == 'true':
        try:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
            password = token.token
        except Exception as e:
            logger.error(f"Failed to get Azure AD token: {e}")
            # Fallback to environment variable if exists
            password = os.environ.get('PGPASSWORD', '')
    else:
        password = os.environ.get('PGPASSWORD', '')
    
    # Fix: Build connection string properly
    host = os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')
    database = os.environ.get('PGDATABASE', 'postgres')
    user = os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')
    
    conn_str = f"host={host} dbname={database} user={user} password={password} sslmode=require"
    
    return conn_str

@app.function_name(name="health")
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    logger.info('Health check endpoint called')
    
    try:
        # Test database connection
        conn_str = get_connection_string()
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        
        return func.HttpResponse(
            json.dumps({
                "status": "healthy",
                "database": "connected",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return func.HttpResponse(
            json.dumps({
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=503,
            headers={"Content-Type": "application/json"}
        )

@app.function_name(name="HttpIngest")
@app.route(route="HttpIngest", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
    """Main ingestion endpoint for LogicMonitor data"""
    logger.info('HTTP Ingest triggered')
    
    try:
        # Get request body
        body = req.get_body()
        
        # Check if content is gzipped
        content_encoding = req.headers.get('content-encoding', '').lower()
        if content_encoding == 'gzip' or (len(body) > 2 and body[:2] == b'\x1f\x8b'):
            try:
                body = gzip.decompress(body)
                logger.info("Decompressed gzipped content")
            except Exception as e:
                logger.warning(f"Failed to decompress: {e}")
        
        # Parse JSON
        try:
            data = json.loads(body)
            logger.info(f"Received data with {len(str(data))} characters")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON payload"}),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        # Store in database
        conn_str = get_connection_string()
        
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                # Create table if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS lm_metrics (
                        id SERIAL PRIMARY KEY,
                        payload JSONB NOT NULL,
                        ingested_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                
                # Insert data
                cur.execute(
                    "INSERT INTO lm_metrics (payload) VALUES (%s)",
                    (Json(data),)
                )
                conn.commit()
                
                logger.info("Data successfully inserted into database")
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": "Data ingested successfully",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
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
    """Root endpoint"""
    return func.HttpResponse(
        json.dumps({
            "service": "LogicMonitor HTTP Ingest",
            "version": "1.0.0",
            "endpoints": [
                "/api/health",
                "/api/HttpIngest"
            ]
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )