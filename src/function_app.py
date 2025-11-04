import azure.functions as func
import logging
import json
import gzip
import os
from datetime import datetime, timezone

# Initialize the FunctionApp
app = func.FunctionApp()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.function_name(name="health")
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint - no database for now"""
    logger.info('Health check endpoint called')
    
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "service": "LogicMonitor HTTP Ingest",
            "database": "disabled for testing",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )

@app.function_name(name="HttpIngest")
@app.route(route="HttpIngest", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
    """Main ingestion endpoint - stores to file for now"""
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
        data = json.loads(body)
        data_size = len(str(data))
        logger.info(f"Received data with {data_size} characters")
        
        # For now, just log it
        logger.info(f"Data received successfully: {data_size} bytes")
        
        # Save to temporary file for inspection (optional)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"/tmp/ingest_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(data, f)
        logger.info(f"Data saved to {filename}")
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": "Data received successfully",
                "size": data_size,
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
            "status": "running",
            "endpoints": [
                "/api/health",
                "/api/HttpIngest"
            ]
        }),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )