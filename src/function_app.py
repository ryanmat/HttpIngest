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
    """Build PostgreSQL connection string - simplified version"""
    # For now, skip Azure AD auth to test basic connectivity
    host = os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')
    database = os.environ.get('PGDATABASE', 'postgres')
    user = os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')
    password = os.environ.get('PGPASSWORD', '')  # You'll need to set this
    
    # If no password is set, return a simple connection test string
    if not password:
        logger.warning("No PGPASSWORD set, database connection will fail")
        # Return a dummy connection string for testing
        return None
    
    conn_str = f"host={host} dbname={database} user={user} password={password} sslmode=require"
    return conn_str

@app.function_name(name="health")
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    logger.info('Health check endpoint called')
    
    # Simplified health check - test database only if configured
    conn_str = get_connection_string()
    
    if conn_str:
        try:
            with psycopg2.connect(conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
            db_status = "connected"
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            db_status = f"error: {str(e)}"
    else:
        db_status = "not configured"
    
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "database": db_status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }),
        status_code=200,
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