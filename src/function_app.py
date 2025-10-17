# src/function_app.py
import logging
import os
import json
import io
import gzip
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2 import pool
import azure.functions as func
from azure.identity import DefaultAzureCredential
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import hashlib
from contextlib import contextmanager
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a global FunctionApp instance
app = func.FunctionApp()

class AzureADPostgreSQLConnection:
    """Handle Azure AD authentication for PostgreSQL"""
    
    @staticmethod
    def get_connection_string():
        """Build connection string with Azure AD token"""
        host = os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')
        database = os.environ.get('PGDATABASE', 'postgres')
        user = os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')
        
        # Check if we should use Azure AD
        use_azure_ad = os.environ.get('USE_AZURE_AD_AUTH', 'true').lower() == 'true'
        
        if use_azure_ad:
            # Get Azure AD token
            credential = DefaultAzureCredential()
            token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
            password = token.token
        else:
            # Fallback to environment variable
            password = os.environ.get('PGPASSWORD', '')
        
        conn_str = (
            f"host={host} "
            f"dbname={database} "
            f"user={user} "
            f"password={password} "
            f"sslmode=require"
        )
        
        return conn_str

class DatabaseManager:
    """Manage database operations with connection pooling"""
    
    def __init__(self):
        self.use_azure_ad = os.environ.get('USE_AZURE_AD_AUTH', 'true').lower() == 'true'
        
    @contextmanager
    def get_connection(self):
        """Get a database connection"""
        conn = None
        try:
            conn_str = AzureADPostgreSQLConnection.get_connection_string()
            conn = psycopg2.connect(conn_str)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def initialize_schema(self):
        """Initialize the database schema for OTLP data"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    schema_sql = """
                    -- Resources table (for devices/systems)
                    CREATE TABLE IF NOT EXISTS resources (
                        id SERIAL PRIMARY KEY,
                        resource_id VARCHAR(500) UNIQUE NOT NULL,
                        hostname VARCHAR(255),
                        display_name VARCHAR(255),
                        company VARCHAR(255),
                        collector_id VARCHAR(255),
                        system_category VARCHAR(255),
                        attributes JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    -- DataSources table (scopes in OTLP)
                    CREATE TABLE IF NOT EXISTS datasources (
                        id SERIAL PRIMARY KEY,
                        datasource_id VARCHAR(500) UNIQUE NOT NULL,
                        name VARCHAR(255),
                        display_name VARCHAR(255),
                        library_name VARCHAR(255),
                        library_version VARCHAR(100),
                        attributes JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    -- Metric Definitions
                    CREATE TABLE IF NOT EXISTS metric_definitions (
                        id SERIAL PRIMARY KEY,
                        metric_name VARCHAR(500) UNIQUE NOT NULL,
                        description TEXT,
                        unit VARCHAR(100),
                        metric_type VARCHAR(50),
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    -- Instances table
                    CREATE TABLE IF NOT EXISTS instances (
                        id SERIAL PRIMARY KEY,
                        resource_id INTEGER REFERENCES resources(id),
                        datasource_id INTEGER REFERENCES datasources(id),
                        instance_name VARCHAR(255),
                        display_name VARCHAR(255),
                        attributes JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(resource_id, datasource_id, instance_name)
                    );
                    
                    -- Time Series Data (partitioned)
                    CREATE TABLE IF NOT EXISTS metric_data (
                        id BIGSERIAL,
                        resource_id INTEGER REFERENCES resources(id),
                        datasource_id INTEGER REFERENCES datasources(id),
                        metric_id INTEGER REFERENCES metric_definitions(id),
                        instance_id INTEGER REFERENCES instances(id),
                        timestamp TIMESTAMPTZ NOT NULL,
                        value DOUBLE PRECISION,
                        attributes JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (id, timestamp)
                    ) PARTITION BY RANGE (timestamp);
                    
                    -- Create current month partition if not exists
                    DO $$
                    DECLARE
                        partition_name TEXT;
                        start_date DATE;
                        end_date DATE;
                    BEGIN
                        start_date := date_trunc('month', CURRENT_DATE);
                        end_date := start_date + interval '1 month';
                        partition_name := 'metric_data_' || to_char(start_date, 'YYYY_MM');
                        
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_tables 
                            WHERE tablename = partition_name
                        ) THEN
                            EXECUTE format(
                                'CREATE TABLE %I PARTITION OF metric_data FOR VALUES FROM (%L) TO (%L)',
                                partition_name, start_date, end_date
                            );
                        END IF;
                    END $$;
                    
                    -- Raw ingestion table (for debugging/replay)
                    CREATE TABLE IF NOT EXISTS raw_otlp_ingestion (
                        id BIGSERIAL PRIMARY KEY,
                        payload JSONB NOT NULL,
                        checksum VARCHAR(64) UNIQUE,
                        processing_status VARCHAR(50) DEFAULT 'pending',
                        error_details JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    -- Legacy table for backward compatibility
                    CREATE TABLE IF NOT EXISTS json_data (
                        id SERIAL PRIMARY KEY,
                        data JSONB NOT NULL,
                        timestamp TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    -- Create indexes
                    CREATE INDEX IF NOT EXISTS idx_metric_data_resource_time 
                        ON metric_data(resource_id, timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_metric_data_metric_time 
                        ON metric_data(metric_id, timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_metric_data_instance_time 
                        ON metric_data(instance_id, timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_resources_hostname 
                        ON resources(hostname);
                    CREATE INDEX IF NOT EXISTS idx_resources_attributes 
                        ON resources USING GIN (attributes);
                    """
                    
                    cursor.execute(schema_sql)
                    logger.info("Database schema initialized successfully")
                    
        except Exception as e:
            logger.error(f"Schema initialization error: {str(e)}")
            if "already exists" not in str(e):
                raise

class OTLPProcessor:
    """Process OTLP-formatted data from LogicMonitor"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        
    def process_otlp_payload(self, payload: Dict) -> Dict:
        """Process the OTLP payload and normalize it"""
        stats = {
            "resources_processed": 0,
            "metrics_processed": 0,
            "datapoints_stored": 0,
            "errors": []
        }
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Process resourceMetrics
                    resource_metrics = payload.get('resourceMetrics', [])
                    
                    for resource_metric in resource_metrics:
                        # Extract resource information
                        resource_data = resource_metric.get('resource', {})
                        resource_attrs = self._extract_attributes(
                            resource_data.get('attributes', [])
                        )
                        
                        # Create unique resource ID
                        resource_id_str = self._create_resource_id(resource_attrs)
                        
                        # Upsert resource
                        cursor.execute("""
                            INSERT INTO resources (
                                resource_id, hostname, display_name, 
                                company, collector_id, system_category, attributes
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (resource_id) 
                            DO UPDATE SET 
                                updated_at = NOW(),
                                attributes = EXCLUDED.attributes
                            RETURNING id
                        """, (
                            resource_id_str,
                            resource_attrs.get('system.hostname'),
                            resource_attrs.get('system.displayname'),
                            resource_attrs.get('company'),
                            resource_attrs.get('collector.id'),
                            resource_attrs.get('system.categories'),
                            Json(resource_attrs)
                        ))
                        resource_db_id = cursor.fetchone()[0]
                        stats["resources_processed"] += 1
                        
                        # Process scopeMetrics
                        scope_metrics_list = resource_metric.get('scopeMetrics', [])
                        
                        for scope_metrics in scope_metrics_list:
                            # Extract scope (datasource) information
                            scope = scope_metrics.get('scope', {})
                            scope_name = scope.get('name', 'unknown')
                            
                            # Create unique datasource ID
                            datasource_id_str = f"{scope_name}_{scope.get('version', '')}"
                            
                            # Upsert datasource
                            cursor.execute("""
                                INSERT INTO datasources (
                                    datasource_id, name, library_name, 
                                    library_version, attributes
                                ) VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (datasource_id) 
                                DO UPDATE SET 
                                    attributes = EXCLUDED.attributes
                                RETURNING id
                            """, (
                                datasource_id_str,
                                scope_name,
                                scope.get('name'),
                                scope.get('version'),
                                Json(scope)
                            ))
                            datasource_db_id = cursor.fetchone()[0]
                            
                            # Process metrics
                            metrics = scope_metrics.get('metrics', [])
                            
                            for metric in metrics:
                                metric_name = metric.get('name', 'unknown')
                                metric_type = self._get_metric_type(metric)
                                
                                # Upsert metric definition
                                cursor.execute("""
                                    INSERT INTO metric_definitions (
                                        metric_name, description, unit, metric_type
                                    ) VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (metric_name) 
                                    DO UPDATE SET 
                                        metric_type = EXCLUDED.metric_type
                                    RETURNING id
                                """, (
                                    metric_name,
                                    metric.get('description'),
                                    metric.get('unit'),
                                    metric_type
                                ))
                                metric_db_id = cursor.fetchone()[0]
                                stats["metrics_processed"] += 1
                                
                                # Process data points based on metric type
                                datapoints = self._extract_datapoints(metric, metric_type)
                                
                                for dp in datapoints:
                                    # Extract instance information from attributes
                                    instance_attrs = dp.get('attributes', {})
                                    instance_name = instance_attrs.get(
                                        'dataSourceInstanceName', 
                                        instance_attrs.get('instance', 'default')
                                    )
                                    
                                    # Upsert instance
                                    cursor.execute("""
                                        INSERT INTO instances (
                                            resource_id, datasource_id, 
                                            instance_name, attributes
                                        ) VALUES (%s, %s, %s, %s)
                                        ON CONFLICT (resource_id, datasource_id, instance_name) 
                                        DO UPDATE SET 
                                            attributes = EXCLUDED.attributes
                                        RETURNING id
                                    """, (
                                        resource_db_id,
                                        datasource_db_id,
                                        instance_name,
                                        Json(instance_attrs)
                                    ))
                                    instance_db_id = cursor.fetchone()[0]
                                    
                                    # Insert time series data
                                    timestamp = datetime.fromtimestamp(
                                        dp['timestamp'] / 1_000_000_000,  # Convert nanoseconds
                                        tz=timezone.utc
                                    )
                                    
                                    cursor.execute("""
                                        INSERT INTO metric_data (
                                            resource_id, datasource_id, metric_id, 
                                            instance_id, timestamp, value, attributes
                                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """, (
                                        resource_db_id,
                                        datasource_db_id,
                                        metric_db_id,
                                        instance_db_id,
                                        timestamp,
                                        dp['value'],
                                        Json(dp.get('attributes', {}))
                                    ))
                                    stats["datapoints_stored"] += 1
                    
                    conn.commit()
                    
        except Exception as e:
            error_msg = f"OTLP processing error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            stats["errors"].append(error_msg)
            
        return stats
    
    def _extract_attributes(self, attributes: List) -> Dict:
        """Extract attributes from OTLP format to dictionary"""
        result = {}
        for attr in attributes:
            key = attr.get('key', '')
            value = attr.get('value', {})
            
            # Handle different value types
            if 'stringValue' in value:
                result[key] = value['stringValue']
            elif 'intValue' in value:
                result[key] = value['intValue']
            elif 'doubleValue' in value:
                result[key] = value['doubleValue']
            elif 'boolValue' in value:
                result[key] = value['boolValue']
            else:
                result[key] = str(value)
                
        return result
    
    def _create_resource_id(self, attrs: Dict) -> str:
        """Create unique resource ID from attributes"""
        # Use combination of key attributes
        key_parts = [
            attrs.get('system.hostname', ''),
            attrs.get('system.deviceId', ''),
            attrs.get('company', ''),
            attrs.get('collector.id', '')
        ]
        return '_'.join(filter(None, key_parts)) or 'unknown'
    
    def _get_metric_type(self, metric: Dict) -> str:
        """Determine metric type from OTLP structure"""
        if 'gauge' in metric:
            return 'gauge'
        elif 'sum' in metric:
            return 'sum'
        elif 'histogram' in metric:
            return 'histogram'
        elif 'summary' in metric:
            return 'summary'
        else:
            return 'unknown'
    
    def _extract_datapoints(self, metric: Dict, metric_type: str) -> List[Dict]:
        """Extract datapoints from metric based on type"""
        datapoints = []
        
        if metric_type == 'gauge' and 'gauge' in metric:
            gauge_data = metric['gauge']
            for dp in gauge_data.get('dataPoints', []):
                datapoints.append({
                    'timestamp': dp.get('timeUnixNano', 0),
                    'value': dp.get('asDouble', dp.get('asInt', 0)),
                    'attributes': self._extract_attributes(dp.get('attributes', []))
                })
                
        elif metric_type == 'sum' and 'sum' in metric:
            sum_data = metric['sum']
            for dp in sum_data.get('dataPoints', []):
                datapoints.append({
                    'timestamp': dp.get('timeUnixNano', 0),
                    'value': dp.get('asDouble', dp.get('asInt', 0)),
                    'attributes': self._extract_attributes(dp.get('attributes', []))
                })
                
        # Add other metric types as needed
        
        return datapoints

# Initialize database manager globally
db_manager = None

def initialize_app():
    """Initialize application components"""
    global db_manager
    if not db_manager:
        db_manager = DatabaseManager()
        db_manager.initialize_schema()

@app.function_name(name="HttpIngest")
@app.route(route="HttpIngest", methods=["POST"])
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
    """Main ingestion endpoint for OTLP data from LogicMonitor"""
    
    logger.info('Processing HTTPS request from LogicMonitor Data Publisher')
    
    # Initialize app
    initialize_app()
    
    try:
        # Read and decompress payload
        body_bytes = req.get_body()
        
        if req.headers.get("Content-Encoding", "").lower() == "gzip":
            logger.info("Decompressing gzipped payload")
            with gzip.GzipFile(fileobj=io.BytesIO(body_bytes)) as f:
                decompressed_bytes = f.read()
            req_body = json.loads(decompressed_bytes.decode('utf-8'))
        else:
            req_body = req.get_json()
            
    except Exception as e:
        logger.error(f"Error parsing JSON payload: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON payload", "details": str(e)}),
            status_code=400,
            mimetype="application/json"
        )
    
    # Check if legacy mode is enabled
    legacy_mode = os.environ.get('LEGACY_MODE', 'false').lower() == 'true'
    
    if legacy_mode:
        # Store in legacy format for backward compatibility
        try:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO json_data (data) VALUES (%s) RETURNING id",
                        (Json(req_body),)
                    )
                    inserted_id = cursor.fetchone()[0]
                    
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "mode": "legacy",
                    "id": inserted_id
                }),
                status_code=200,
                mimetype="application/json"
            )
        except Exception as e:
            logger.error(f"Legacy storage error: {e}")
            return func.HttpResponse(
                json.dumps({"error": str(e)}),
                status_code=500,
                mimetype="application/json"
            )
    
    # Process OTLP data
    try:
        processor = OTLPProcessor(db_manager)
        
        # Store raw for debugging
        checksum = hashlib.sha256(
            json.dumps(req_body, sort_keys=True).encode()
        ).hexdigest()
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO raw_otlp_ingestion (payload, checksum)
                    VALUES (%s, %s)
                    ON CONFLICT (checksum) DO NOTHING
                    RETURNING id
                """, (Json(req_body), checksum))
                
                result = cursor.fetchone()
                if not result:
                    return func.HttpResponse(
                        json.dumps({
                            "status": "duplicate",
                            "checksum": checksum
                        }),
                        status_code=200,
                        mimetype="application/json"
                    )
        
        # Process the OTLP payload
        stats = processor.process_otlp_payload(req_body)
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "checksum": checksum,
                "stats": stats,
                "timestamp": datetime.utcnow().isoformat()
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.error(f"Processing error: {e}\n{traceback.format_exc()}")
        return func.HttpResponse(
            json.dumps({
                "error": "Processing failed",
                "details": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    
    health = {
        "status": "healthy",
        "service": "lm-otlp-ingest",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    try:
        initialize_app()
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM resources")
                resource_count = cursor.fetchone()[0]
                health["checks"]["database"] = "connected"
                health["checks"]["resources"] = resource_count
    except Exception as e:
        health["status"] = "unhealthy"
        health["checks"]["database"] = f"error: {str(e)}"
        
    status_code = 200 if health["status"] == "healthy" else 503
    
    return func.HttpResponse(
        json.dumps(health),
        status_code=status_code,
        mimetype="application/json"
    )

@app.route(route="metrics", methods=["GET"])
def get_metrics(req: func.HttpRequest) -> func.HttpResponse:
    """Get ingestion metrics"""
    
    initialize_app()
    
    try:
        with db_manager.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                metrics = {}
                
                # Get counts
                cursor.execute("SELECT COUNT(*) as count FROM resources")
                metrics['resources'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM datasources")
                metrics['datasources'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM metric_definitions")
                metrics['metric_types'] = cursor.fetchone()['count']
                
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM metric_data 
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                """)
                metrics['datapoints_last_hour'] = cursor.fetchone()['count']
                
                # Get top resources by metric count
                cursor.execute("""
                    SELECT 
                        r.hostname,
                        COUNT(md.id) as metric_count
                    FROM resources r
                    LEFT JOIN metric_data md ON r.id = md.resource_id
                    WHERE md.timestamp > NOW() - INTERVAL '1 hour'
                    GROUP BY r.hostname
                    ORDER BY metric_count DESC
                    LIMIT 10
                """)
                metrics['top_resources'] = cursor.fetchall()
                
                metrics['timestamp'] = datetime.utcnow().isoformat()
                
                return func.HttpResponse(
                    json.dumps(metrics, default=str),
                    status_code=200,
                    mimetype="application/json"
                )
                
    except Exception as e:
        logger.error(f"Metrics error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="query", methods=["POST"])
def query_metrics(req: func.HttpRequest) -> func.HttpResponse:
    """Query metrics with filters"""
    
    initialize_app()
    
    try:
        params = req.get_json()
        hostname = params.get('hostname')
        metric_name = params.get('metric')
        start_time = params.get('start_time')
        end_time = params.get('end_time')
        
        with db_manager.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.hostname,
                        d.name as datasource,
                        m.metric_name,
                        i.instance_name,
                        md.timestamp,
                        md.value
                    FROM metric_data md
                    JOIN resources r ON md.resource_id = r.id
                    JOIN datasources d ON md.datasource_id = d.id
                    JOIN metric_definitions m ON md.metric_id = m.id
                    JOIN instances i ON md.instance_id = i.id
                    WHERE 1=1
                """
                
                query_params = []
                
                if hostname:
                    query += " AND r.hostname = %s"
                    query_params.append(hostname)
                    
                if metric_name:
                    query += " AND m.metric_name = %s"
                    query_params.append(metric_name)
                    
                if start_time:
                    query += " AND md.timestamp >= %s"
                    query_params.append(start_time)
                    
                if end_time:
                    query += " AND md.timestamp <= %s"
                    query_params.append(end_time)
                    
                query += " ORDER BY md.timestamp DESC LIMIT 1000"
                
                cursor.execute(query, query_params)
                results = cursor.fetchall()
                
                return func.HttpResponse(
                    json.dumps({"data": results}, default=str),
                    status_code=200,
                    mimetype="application/json"
                )
                
    except Exception as e:
        logger.error(f"Query error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )