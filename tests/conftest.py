# Description: Pytest configuration and fixtures for LogicMonitor data pipeline tests
# Description: Provides database connections, sample data, cleanup, and mocked authentication

import pytest
import psycopg2
import json
import os
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock
from typing import Generator, Dict, Any, Optional


def get_azure_token() -> Optional[str]:
    """
    Get a real Azure AD token for database authentication.

    Returns None if token acquisition fails.
    """
    try:
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "https://ossrdbms-aad.database.windows.net",
             "--query", "accessToken", "--output", "tsv"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


@pytest.fixture(scope="session")
def test_db_config() -> Dict[str, str]:
    """
    Test database configuration.

    Uses environment variables or defaults.
    For real database tests, requires valid Azure AD token.
    """
    return {
        "host": os.environ.get('TEST_PGHOST', os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')),
        "database": os.environ.get('TEST_PGDATABASE', os.environ.get('PGDATABASE', 'postgres')),
        "user": os.environ.get('TEST_PGUSER', os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')),
        "port": os.environ.get('TEST_PGPORT', os.environ.get('PGPORT', '5432')),
    }


@pytest.fixture(scope="session")
def azure_token() -> Optional[str]:
    """
    Get real Azure AD token for testing.

    Returns None if token cannot be obtained.
    Tests requiring database access will be skipped if this is None.
    """
    token = os.environ.get('TEST_PGPASSWORD') or os.environ.get('PGPASSWORD')
    if not token:
        token = get_azure_token()
    return token


@pytest.fixture(scope="function")
def mock_azure_token(monkeypatch):
    """
    Mock Azure AD token acquisition for unit tests.

    Patches subprocess.run to return a fake token.
    Use this for testing code paths that use Azure auth without hitting the database.
    """
    def mock_run(*args, **kwargs):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "test_mock_azure_ad_token_12345"
        mock_result.stderr = ""
        return mock_result

    monkeypatch.setattr("subprocess.run", mock_run)
    return "test_mock_azure_ad_token_12345"


@pytest.fixture(scope="function")
def db_connection(test_db_config: Dict[str, str], azure_token: Optional[str]) -> Generator:
    """
    PostgreSQL database connection for testing.

    Creates a connection to the test database with Azure AD auth.
    Automatically commits transactions and closes connection after test.
    Uses function scope to ensure isolation between tests.
    Skips test if azure_token is not available or connection fails.
    """
    if not azure_token:
        pytest.skip("Azure AD token not available for database testing")

    conn_str = (
        f"host={test_db_config['host']} "
        f"port={test_db_config['port']} "
        f"dbname={test_db_config['database']} "
        f"user={test_db_config['user']} "
        f"password={azure_token} "
        f"sslmode=require"
    )

    try:
        conn = psycopg2.connect(conn_str)
        conn.autocommit = False
    except psycopg2.OperationalError as e:
        pytest.skip(f"Database connection failed (VPN/firewall issue?): {str(e)[:100]}")

    yield conn

    conn.close()


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """
    Database cursor for executing queries.

    Provides a cursor from the db_connection fixture.
    Automatically commits and closes cursor after test.
    """
    cursor = db_connection.cursor()
    yield cursor
    db_connection.commit()
    cursor.close()


@pytest.fixture(scope="function")
def clean_test_table(db_connection) -> Generator:
    """
    Ensures clean database state between tests.

    Creates a temporary test table before each test and drops it after.
    Prevents test data pollution and ensures isolation.
    """
    test_table = "lm_metrics_test"

    with db_connection.cursor() as cur:
        # Create test table with same structure as production
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {test_table} (
                id SERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        db_connection.commit()

    yield test_table

    # Cleanup: Drop test table after test
    with db_connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {test_table}")
        db_connection.commit()


@pytest.fixture(scope="session")
def sample_otlp_data() -> Dict[str, Any]:
    """
    Load sample OTLP JSON data for testing.

    Reads from tests/fixtures/sample_otlp.json.
    Returns the parsed JSON as a dictionary.
    """
    fixture_path = Path(__file__).parent / "fixtures" / "sample_otlp.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


@pytest.fixture(scope="session")
def sample_otlp_cpu_metrics() -> Dict[str, Any]:
    """
    Sample OTLP data with CPU metrics.

    Represents typical CPU usage metrics from LogicMonitor.
    """
    return {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "web-server"}},
                    {"key": "host.name", "value": {"stringValue": "server01"}}
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "CPU_Usage", "version": "1.0"},
                "metrics": [{
                    "name": "cpu.usage",
                    "unit": "percent",
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": 1699123456000000000,
                            "asDouble": 45.2
                        }]
                    }
                }]
            }]
        }]
    }


@pytest.fixture(scope="session")
def sample_otlp_memory_metrics() -> Dict[str, Any]:
    """
    Sample OTLP data with memory metrics.

    Represents typical memory usage metrics from LogicMonitor.
    """
    return {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "database"}},
                    {"key": "host.name", "value": {"stringValue": "db-server-01"}}
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "Memory_Usage", "version": "1.0"},
                "metrics": [{
                    "name": "memory.usage",
                    "unit": "bytes",
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": 1699123456000000000,
                            "asInt": 8589934592
                        }]
                    }
                }]
            }]
        }]
    }


@pytest.fixture(scope="session")
def sample_otlp_multi_metric() -> Dict[str, Any]:
    """
    Sample OTLP data with multiple metrics across multiple resources.

    Tests complex scenarios with multiple services and metric types.
    """
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "web-server"}},
                        {"key": "host.name", "value": {"stringValue": "server01"}},
                        {"key": "environment", "value": {"stringValue": "production"}}
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "CPU_Usage", "version": "1.0"},
                        "metrics": [{
                            "name": "cpu.usage",
                            "unit": "percent",
                            "gauge": {
                                "dataPoints": [{
                                    "timeUnixNano": 1699123456000000000,
                                    "asDouble": 45.2
                                }]
                            }
                        }]
                    },
                    {
                        "scope": {"name": "Memory_Usage", "version": "1.0"},
                        "metrics": [{
                            "name": "memory.usage",
                            "unit": "bytes",
                            "gauge": {
                                "dataPoints": [{
                                    "timeUnixNano": 1699123456000000000,
                                    "asInt": 4294967296
                                }]
                            }
                        }]
                    }
                ]
            },
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "database"}},
                        {"key": "host.name", "value": {"stringValue": "db-server-01"}},
                        {"key": "environment", "value": {"stringValue": "production"}}
                    ]
                },
                "scopeMetrics": [{
                    "scope": {"name": "Disk_Usage", "version": "1.0"},
                    "metrics": [{
                        "name": "disk.usage",
                        "unit": "percent",
                        "gauge": {
                            "dataPoints": [{
                                "timeUnixNano": 1699123456000000000,
                                "asDouble": 78.5
                            }]
                        }
                    }]
                }]
            }
        ]
    }


@pytest.fixture
def clean_normalized_tables(db_connection):
    """
    Clean up normalized tables and test lm_metrics records before and after test.

    Creates a clean state for testing by removing test data.
    """
    with db_connection.cursor() as cur:
        # Delete in order due to foreign keys
        cur.execute("DELETE FROM processing_status")
        cur.execute("DELETE FROM metric_data")
        cur.execute("DELETE FROM metric_definitions")
        cur.execute("DELETE FROM datasources")
        cur.execute("DELETE FROM resources")
        # Clean test records from lm_metrics (those with test-related content in payload)
        cur.execute("""
            DELETE FROM lm_metrics
            WHERE payload::text LIKE '%test%'
               OR payload::text LIKE '%web-server%'
               OR payload::text LIKE '%database%'
               OR payload::text LIKE '%invalid%'
               OR payload::text LIKE '%memory%'
        """)
        db_connection.commit()

    yield

    # Cleanup after test
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM processing_status")
        cur.execute("DELETE FROM metric_data")
        cur.execute("DELETE FROM metric_definitions")
        cur.execute("DELETE FROM datasources")
        cur.execute("DELETE FROM resources")
        cur.execute("""
            DELETE FROM lm_metrics
            WHERE payload::text LIKE '%test%'
               OR payload::text LIKE '%web-server%'
               OR payload::text LIKE '%database%'
               OR payload::text LIKE '%invalid%'
               OR payload::text LIKE '%memory%'
        """)
        db_connection.commit()


# ============================================================================
# Aggregator Test Fixtures
# ============================================================================

@pytest.fixture
def setup_test_metric_data(db_connection, clean_normalized_tables):
    """
    Create basic metric data for aggregator testing.

    Returns resource_id and metric_definition_id for the created data.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('TestAggregatorDS', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('aggregator_test_hash', '{"service.name": "aggregator-test"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'aggregator.test', 'count', 'gauge', 'Aggregator test metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert recent data (within last hour)
        now = datetime.now(timezone.utc)
        for i in range(10):
            timestamp = now - timedelta(minutes=i * 5)
            value = 50.0 + i

            cur.execute(
                """
                INSERT INTO metric_data (
                    resource_id, metric_definition_id, timestamp,
                    value_double, attributes, created_at
                )
                VALUES (%s, %s, %s, %s, '{}', NOW())
                """,
                (resource_id, metric_def_id, timestamp, value)
            )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_test_metric_data_multiple_hours(db_connection, clean_normalized_tables):
    """
    Create metric data spanning multiple hours.

    Returns resource_id and metric_definition_id for the created data.
    """
    from datetime import datetime, timezone, timedelta

    # Create resource and datasource
    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('TestDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('test_hash_123', '{"service.name": "test-service"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'test.metric', 'count', 'gauge', 'Test metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert data across 5 hours
        now = datetime.now(timezone.utc)
        for hour_offset in range(5):
            timestamp = now - timedelta(hours=hour_offset)

            # Insert 3 data points per hour
            for i in range(3):
                point_time = timestamp + timedelta(minutes=i * 20)
                value = 50.0 + hour_offset * 10 + i

                cur.execute(
                    """
                    INSERT INTO metric_data (
                        resource_id, metric_definition_id, timestamp,
                        value_double, attributes, created_at
                    )
                    VALUES (%s, %s, %s, %s, '{}', NOW())
                    """,
                    (resource_id, metric_def_id, point_time, value)
                )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_known_metric_values(db_connection, clean_normalized_tables):
    """
    Create metric data with known statistics for accuracy testing.

    Returns resource_id, metric_definition_id, and expected statistics.
    """
    from datetime import datetime, timezone, timedelta

    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    expected_stats = {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / len(values),
        "sum": sum(values),
    }

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('KnownDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('known_hash_123', '{"service.name": "known-service"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'known.metric', 'count', 'gauge', 'Known metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert known values - all within same hour to avoid boundary issues
        # Use 30 minutes ago as base, then go back from there to ensure all in past
        now = datetime.now(timezone.utc)
        base_time = now - timedelta(minutes=30)
        base_time = base_time.replace(second=0, microsecond=0)

        for i, value in enumerate(values):
            # All data points in the past (30, 31, 32, 33, 34 minutes ago)
            timestamp = base_time - timedelta(minutes=i)

            cur.execute(
                """
                INSERT INTO metric_data (
                    resource_id, metric_definition_id, timestamp,
                    value_double, attributes, created_at
                )
                VALUES (%s, %s, %s, %s, '{}', NOW())
                """,
                (resource_id, metric_def_id, timestamp, value)
            )

        db_connection.commit()

    return resource_id, metric_def_id, expected_stats


@pytest.fixture
def setup_hourly_rollups(db_connection, clean_normalized_tables):
    """
    Create hourly rollup data for daily summary testing.

    Returns resource_id and metric_definition_id.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('RollupDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('rollup_hash_123', '{"service.name": "rollup-service"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'rollup.metric', 'count', 'gauge', 'Rollup metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Create hourly_rollups table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hourly_rollups (
                id SERIAL PRIMARY KEY,
                resource_id INTEGER NOT NULL,
                metric_definition_id INTEGER NOT NULL,
                hour TIMESTAMPTZ NOT NULL,
                data_point_count INTEGER NOT NULL,
                min_value DOUBLE PRECISION,
                max_value DOUBLE PRECISION,
                avg_value DOUBLE PRECISION,
                sum_value DOUBLE PRECISION,
                stddev_value DOUBLE PRECISION,
                first_timestamp TIMESTAMPTZ,
                last_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(resource_id, metric_definition_id, hour)
            )
        """)

        # Insert hourly rollups for yesterday
        yesterday = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

        for hour_offset in range(10):
            hour = yesterday + timedelta(hours=hour_offset)

            cur.execute(
                """
                INSERT INTO hourly_rollups (
                    resource_id, metric_definition_id, hour,
                    data_point_count, min_value, max_value, avg_value, sum_value,
                    first_timestamp, last_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (resource_id, metric_def_id, hour, 60, 10.0, 100.0, 55.0, 3300.0, hour, hour + timedelta(minutes=59))
            )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_hourly_rollups_known_values(db_connection, clean_normalized_tables):
    """
    Create hourly rollups with known statistics for accuracy testing.

    Returns resource_id, metric_definition_id, and expected statistics.
    """
    from datetime import datetime, timezone, timedelta

    hour_count = 5
    expected_stats = {
        "hour_count": hour_count,
        "total_points": hour_count * 60,
        "min": 10.0,
        "max": 100.0,
    }

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('KnownRollupDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('known_rollup_hash', '{"service.name": "known-rollup"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'known.rollup', 'count', 'gauge', 'Known rollup')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Create hourly_rollups table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hourly_rollups (
                id SERIAL PRIMARY KEY,
                resource_id INTEGER NOT NULL,
                metric_definition_id INTEGER NOT NULL,
                hour TIMESTAMPTZ NOT NULL,
                data_point_count INTEGER NOT NULL,
                min_value DOUBLE PRECISION,
                max_value DOUBLE PRECISION,
                avg_value DOUBLE PRECISION,
                sum_value DOUBLE PRECISION,
                stddev_value DOUBLE PRECISION,
                first_timestamp TIMESTAMPTZ,
                last_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(resource_id, metric_definition_id, hour)
            )
        """)

        # Insert known hourly rollups
        yesterday = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

        for hour_offset in range(hour_count):
            hour = yesterday + timedelta(hours=hour_offset)

            cur.execute(
                """
                INSERT INTO hourly_rollups (
                    resource_id, metric_definition_id, hour,
                    data_point_count, min_value, max_value, avg_value, sum_value,
                    first_timestamp, last_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (resource_id, metric_def_id, hour, 60, 10.0, 100.0, 55.0, 3300.0, hour, hour + timedelta(minutes=59))
            )

        db_connection.commit()

    return resource_id, metric_def_id, expected_stats


@pytest.fixture
def setup_continuous_metric_data(db_connection, clean_normalized_tables):
    """
    Create continuous metric data without gaps.

    Returns resource_id and metric_definition_id.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('ContinuousDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('continuous_hash', '{"service.name": "continuous"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'continuous.metric', 'count', 'gauge', 'Continuous metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert continuous data (every hour for 5 hours)
        now = datetime.now(timezone.utc)
        for hour_offset in range(5):
            timestamp = now - timedelta(hours=hour_offset)

            cur.execute(
                """
                INSERT INTO metric_data (
                    resource_id, metric_definition_id, timestamp,
                    value_double, attributes, created_at
                )
                VALUES (%s, %s, %s, %s, '{}', NOW())
                """,
                (resource_id, metric_def_id, timestamp, 50.0)
            )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_metric_data_with_gaps(db_connection, clean_normalized_tables):
    """
    Create metric data with intentional gaps.

    Returns resource_id and metric_definition_id.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('GappyDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('gappy_hash', '{"service.name": "gappy"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'gappy.metric', 'count', 'gauge', 'Gappy metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert data with gaps (hour 0, skip 1-2, hour 3, skip 4-5, hour 6)
        now = datetime.now(timezone.utc)
        for hour_offset in [0, 3, 6, 9]:
            timestamp = now - timedelta(hours=hour_offset)

            cur.execute(
                """
                INSERT INTO metric_data (
                    resource_id, metric_definition_id, timestamp,
                    value_double, attributes, created_at
                )
                VALUES (%s, %s, %s, %s, '{}', NOW())
                """,
                (resource_id, metric_def_id, timestamp, 50.0)
            )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_5min_interval_data(db_connection, clean_normalized_tables):
    """
    Create metric data with 5-minute intervals.

    Returns resource_id and metric_definition_id.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('FiveMinDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('fivemin_hash', '{"service.name": "fivemin"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'fivemin.metric', 'count', 'gauge', 'Five min metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert data every 5 minutes for 2 hours
        now = datetime.now(timezone.utc)
        for min_offset in range(0, 121, 5):
            timestamp = now - timedelta(minutes=min_offset)

            cur.execute(
                """
                INSERT INTO metric_data (
                    resource_id, metric_definition_id, timestamp,
                    value_double, attributes, created_at
                )
                VALUES (%s, %s, %s, %s, '{}', NOW())
                """,
                (resource_id, metric_def_id, timestamp, 50.0)
            )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_old_metric_data(db_connection, clean_normalized_tables):
    """
    Create old metric data for retention policy testing.

    Returns resource_id and metric_definition_id.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('OldDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('old_hash', '{"service.name": "old"}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'old.metric', 'count', 'gauge', 'Old metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert old data (10 days ago)
        old_timestamp = datetime.now(timezone.utc) - timedelta(days=10)

        cur.execute(
            """
            INSERT INTO metric_data (
                resource_id, metric_definition_id, timestamp,
                value_double, attributes, created_at
            )
            VALUES (%s, %s, %s, %s, '{}', NOW())
            """,
            (resource_id, metric_def_id, old_timestamp, 50.0)
        )

        # Also insert recent data
        recent_timestamp = datetime.now(timezone.utc)
        cur.execute(
            """
            INSERT INTO metric_data (
                resource_id, metric_definition_id, timestamp,
                value_double, attributes, created_at
            )
            VALUES (%s, %s, %s, %s, '{}', NOW())
            """,
            (resource_id, metric_def_id, recent_timestamp, 75.0)
        )

        db_connection.commit()

    return resource_id, metric_def_id


@pytest.fixture
def setup_old_hourly_rollups(db_connection, clean_normalized_tables):
    """
    Create old hourly rollups for retention policy testing.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('OldRollupDS', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('old_rollup_hash', '{}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'old.rollup', 'count', 'gauge', 'Old rollup')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Create hourly_rollups table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hourly_rollups (
                id SERIAL PRIMARY KEY,
                resource_id INTEGER NOT NULL,
                metric_definition_id INTEGER NOT NULL,
                hour TIMESTAMPTZ NOT NULL,
                data_point_count INTEGER NOT NULL,
                min_value DOUBLE PRECISION,
                max_value DOUBLE PRECISION,
                avg_value DOUBLE PRECISION,
                sum_value DOUBLE PRECISION,
                stddev_value DOUBLE PRECISION,
                first_timestamp TIMESTAMPTZ,
                last_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(resource_id, metric_definition_id, hour)
            )
        """)

        # Insert old hourly rollup (100 days ago)
        old_hour = datetime.now(timezone.utc) - timedelta(days=100)

        cur.execute(
            """
            INSERT INTO hourly_rollups (
                resource_id, metric_definition_id, hour,
                data_point_count, min_value, max_value, avg_value, sum_value,
                first_timestamp, last_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (resource_id, metric_def_id, old_hour, 60, 10.0, 100.0, 55.0, 3300.0, old_hour, old_hour + timedelta(minutes=59))
        )

        db_connection.commit()


@pytest.fixture
def setup_old_daily_summaries(db_connection, clean_normalized_tables):
    """
    Create old daily summaries for retention policy testing.
    """
    from datetime import datetime, timezone, timedelta

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('OldSummaryDS', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('old_summary_hash', '{}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'old.summary', 'count', 'gauge', 'Old summary')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Create daily_summaries table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id SERIAL PRIMARY KEY,
                resource_id INTEGER NOT NULL,
                metric_definition_id INTEGER NOT NULL,
                day DATE NOT NULL,
                hour_count INTEGER NOT NULL,
                total_data_points INTEGER NOT NULL,
                min_value DOUBLE PRECISION,
                max_value DOUBLE PRECISION,
                avg_value DOUBLE PRECISION,
                sum_value DOUBLE PRECISION,
                stddev_value DOUBLE PRECISION,
                first_hour TIMESTAMPTZ,
                last_hour TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(resource_id, metric_definition_id, day)
            )
        """)

        # Insert old daily summary (400 days ago)
        old_day = datetime.now(timezone.utc) - timedelta(days=400)

        cur.execute(
            """
            INSERT INTO daily_summaries (
                resource_id, metric_definition_id, day,
                hour_count, total_data_points, min_value, max_value, avg_value, sum_value,
                first_hour, last_hour
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (resource_id, metric_def_id, old_day.date(), 24, 1440, 10.0, 100.0, 55.0, 79200.0, old_day, old_day + timedelta(hours=23))
        )

        db_connection.commit()


@pytest.fixture
def setup_metric_data_with_nulls(db_connection, clean_normalized_tables):
    """
    Create metric data with NULL values.
    """
    from datetime import datetime, timezone

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('NullDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('null_hash', '{}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'null.metric', 'count', 'gauge', 'Null metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert NULL value
        cur.execute(
            """
            INSERT INTO metric_data (
                resource_id, metric_definition_id, timestamp,
                value_double, value_int, attributes, created_at
            )
            VALUES (%s, %s, %s, NULL, NULL, '{}', NOW())
            """,
            (resource_id, metric_def_id, datetime.now(timezone.utc))
        )

        db_connection.commit()


@pytest.fixture
def setup_metric_data_with_duplicates(db_connection, clean_normalized_tables):
    """
    Create metric data with duplicate timestamps.
    """
    from datetime import datetime, timezone

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('DupeDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('dupe_hash', '{}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'dupe.metric', 'count', 'gauge', 'Dupe metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert duplicate timestamps
        timestamp = datetime.now(timezone.utc)
        for _ in range(2):
            cur.execute(
                """
                INSERT INTO metric_data (
                    resource_id, metric_definition_id, timestamp,
                    value_double, attributes, created_at
                )
                VALUES (%s, %s, %s, %s, '{}', NOW())
                """,
                (resource_id, metric_def_id, timestamp, 50.0)
            )

        db_connection.commit()


@pytest.fixture
def setup_single_metric_datapoint(db_connection, clean_normalized_tables):
    """
    Create a single metric data point.
    """
    from datetime import datetime, timezone

    with db_connection.cursor() as cur:
        # Create datasource
        cur.execute(
            """
            INSERT INTO datasources (name, version, created_at)
            VALUES ('SingleDataSource', '1.0', NOW())
            RETURNING id
            """
        )
        datasource_id = cur.fetchone()[0]

        # Create resource
        cur.execute(
            """
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('single_hash', '{}', NOW(), NOW())
            RETURNING id
            """
        )
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute(
            """
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'single.metric', 'count', 'gauge', 'Single metric')
            RETURNING id
            """,
            (datasource_id,)
        )
        metric_def_id = cur.fetchone()[0]

        # Insert single data point
        cur.execute(
            """
            INSERT INTO metric_data (
                resource_id, metric_definition_id, timestamp,
                value_double, attributes, created_at
            )
            VALUES (%s, %s, %s, %s, '{}', NOW())
            """,
            (resource_id, metric_def_id, datetime.now(timezone.utc), 50.0)
        )

        db_connection.commit()

    return resource_id, metric_def_id
