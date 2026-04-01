# Description: Pytest configuration and fixtures for LogicMonitor data pipeline tests
# Description: Provides database connections, sample data, cleanup, and mocked authentication

import pytest
import psycopg2
import json
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock
from typing import Generator, Dict, Any, Optional


def get_azure_token() -> Optional[str]:
    """
    Get a real Azure AD token for database authentication.

    Returns None if token acquisition fails.
    """
    try:
        result = subprocess.run(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "https://ossrdbms-aad.database.windows.net",
                "--query",
                "accessToken",
                "--output",
                "tsv",
            ],
            capture_output=True,
            text=True,
            timeout=10,
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
        "host": os.environ.get(
            "TEST_PGHOST",
            os.environ.get("PGHOST", "rm-postgres.postgres.database.azure.com"),
        ),
        "database": os.environ.get(
            "TEST_PGDATABASE", os.environ.get("PGDATABASE", "postgres")
        ),
        "user": os.environ.get(
            "TEST_PGUSER", os.environ.get("PGUSER", "ryan.matuszewski@logicmonitor.com")
        ),
        "port": os.environ.get("TEST_PGPORT", os.environ.get("PGPORT", "5432")),
    }


@pytest.fixture(scope="session")
def azure_token() -> Optional[str]:
    """
    Get real Azure AD token for testing.

    Returns None if token cannot be obtained.
    Tests requiring database access will be skipped if this is None.
    """
    token = os.environ.get("TEST_PGPASSWORD") or os.environ.get("PGPASSWORD")
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
def db_connection(
    test_db_config: Dict[str, str], azure_token: Optional[str]
) -> Generator:
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
    with open(fixture_path, "r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def sample_otlp_cpu_metrics() -> Dict[str, Any]:
    """
    Sample OTLP data with CPU metrics.

    Represents typical CPU usage metrics from LogicMonitor.
    """
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "web-server"}},
                        {"key": "host.name", "value": {"stringValue": "server01"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "CPU_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "cpu.usage",
                                "unit": "percent",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asDouble": 45.2,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        ]
    }


@pytest.fixture(scope="session")
def sample_otlp_memory_metrics() -> Dict[str, Any]:
    """
    Sample OTLP data with memory metrics.

    Represents typical memory usage metrics from LogicMonitor.
    """
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "database"}},
                        {"key": "host.name", "value": {"stringValue": "db-server-01"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "Memory_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "memory.usage",
                                "unit": "bytes",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asInt": 8589934592,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        ]
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
                        {"key": "environment", "value": {"stringValue": "production"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "CPU_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "cpu.usage",
                                "unit": "percent",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asDouble": 45.2,
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                    {
                        "scope": {"name": "Memory_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "memory.usage",
                                "unit": "bytes",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asInt": 4294967296,
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "database"}},
                        {"key": "host.name", "value": {"stringValue": "db-server-01"}},
                        {"key": "environment", "value": {"stringValue": "production"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "Disk_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "disk.usage",
                                "unit": "percent",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asDouble": 78.5,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            },
        ]
    }
