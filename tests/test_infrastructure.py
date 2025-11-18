# Description: Tests to verify pytest infrastructure and fixtures work correctly
# Description: Validates database connections, sample data loading, and test isolation

import pytest
import json


def test_mock_azure_token_fixture(mock_azure_token):
    """Verify Azure AD token mocking works."""
    assert mock_azure_token == "test_mock_azure_ad_token_12345"
    assert isinstance(mock_azure_token, str)


def test_db_connection_fixture(db_connection):
    """Verify database connection fixture works."""
    assert db_connection is not None
    assert not db_connection.closed

    # Test basic query execution
    with db_connection.cursor() as cur:
        cur.execute("SELECT 1 as test")
        result = cur.fetchone()
        assert result[0] == 1


def test_db_cursor_fixture(db_cursor):
    """Verify database cursor fixture works."""
    assert db_cursor is not None

    # Test query execution
    db_cursor.execute("SELECT 2 + 2 as result")
    result = db_cursor.fetchone()
    assert result[0] == 4


def test_clean_test_table_fixture(db_connection, clean_test_table):
    """Verify test table creation and cleanup works."""
    test_table = clean_test_table

    # Verify table exists
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (test_table,))
        exists = cur.fetchone()[0]
        assert exists is True

    # Verify we can insert data
    with db_connection.cursor() as cur:
        cur.execute(
            f"INSERT INTO {test_table} (payload) VALUES (%s) RETURNING id",
            (json.dumps({"test": "data"}),)
        )
        insert_id = cur.fetchone()[0]
        assert insert_id > 0
        db_connection.commit()


def test_sample_otlp_data_fixture(sample_otlp_data):
    """Verify sample OTLP data fixture loads correctly."""
    assert sample_otlp_data is not None
    assert "resourceMetrics" in sample_otlp_data
    assert isinstance(sample_otlp_data["resourceMetrics"], list)
    assert len(sample_otlp_data["resourceMetrics"]) > 0


def test_sample_otlp_cpu_metrics_fixture(sample_otlp_cpu_metrics):
    """Verify CPU metrics fixture has correct structure."""
    assert "resourceMetrics" in sample_otlp_cpu_metrics

    resource_metrics = sample_otlp_cpu_metrics["resourceMetrics"][0]
    assert "resource" in resource_metrics
    assert "scopeMetrics" in resource_metrics

    scope_metrics = resource_metrics["scopeMetrics"][0]
    assert scope_metrics["scope"]["name"] == "CPU_Usage"

    metric = scope_metrics["metrics"][0]
    assert metric["name"] == "cpu.usage"
    assert metric["unit"] == "percent"
    assert "gauge" in metric


def test_sample_otlp_memory_metrics_fixture(sample_otlp_memory_metrics):
    """Verify memory metrics fixture has correct structure."""
    resource_metrics = sample_otlp_memory_metrics["resourceMetrics"][0]
    scope_metrics = resource_metrics["scopeMetrics"][0]

    assert scope_metrics["scope"]["name"] == "Memory_Usage"

    metric = scope_metrics["metrics"][0]
    assert metric["name"] == "memory.usage"
    assert metric["unit"] == "bytes"


def test_sample_otlp_multi_metric_fixture(sample_otlp_multi_metric):
    """Verify multi-metric fixture has multiple resources and metrics."""
    assert len(sample_otlp_multi_metric["resourceMetrics"]) == 2

    # First resource should have 2 scope metrics
    first_resource = sample_otlp_multi_metric["resourceMetrics"][0]
    assert len(first_resource["scopeMetrics"]) == 2

    # Second resource should have 1 scope metric
    second_resource = sample_otlp_multi_metric["resourceMetrics"][1]
    assert len(second_resource["scopeMetrics"]) == 1


def test_fixtures_isolation(db_connection, clean_test_table):
    """
    Verify test isolation works correctly.

    This test inserts data and verifies it exists.
    The next test should not see this data due to clean_test_table fixture.
    """
    test_table = clean_test_table

    with db_connection.cursor() as cur:
        cur.execute(
            f"INSERT INTO {test_table} (payload) VALUES (%s)",
            (json.dumps({"isolation": "test1"}),)
        )
        db_connection.commit()

        cur.execute(f"SELECT COUNT(*) FROM {test_table}")
        count = cur.fetchone()[0]
        assert count == 1


def test_fixtures_isolation_verification(db_connection, clean_test_table):
    """
    Verify previous test data doesn't leak.

    Because clean_test_table recreates the table for each test,
    this should start with an empty table.
    """
    test_table = clean_test_table

    with db_connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {test_table}")
        count = cur.fetchone()[0]
        assert count == 0, "Test table should be empty due to fixture cleanup"
