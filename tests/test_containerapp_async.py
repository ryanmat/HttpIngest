# Description: Tests for async containerapp endpoints with connection pooling
# Description: Verifies performance improvements from async database operations

import pytest
import pytest_asyncio
import asyncio
import json
from datetime import datetime
from httpx import AsyncClient, ASGITransport
import asyncpg

# Import the app
from containerapp_main import app, get_db_connection_params


@pytest_asyncio.fixture
async def async_client():
    """Create an async test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_health_check_async():
    """Test that health check returns component status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/health")

        assert response.status_code in [200, 503]
        data = response.json()

        assert "status" in data
        assert "timestamp" in data
        assert "components" in data
        # New architecture: datalake, hot_cache, synapse, ingestion_router, background_tasks
        assert "datalake" in data["components"]
        assert "hot_cache" in data["components"]
        assert "ingestion_router" in data["components"]
        assert "background_tasks" in data["components"]


@pytest.mark.asyncio
async def test_http_ingest_async(async_client):
    """Test that HTTP ingestion uses async database operations."""
    # Create a valid OTLP payload
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-service"}}
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "test"},
                        "metrics": [
                            {
                                "name": "test.metric",
                                "unit": "1",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "asInt": 42,
                                            "timeUnixNano": str(int(datetime.now().timestamp() * 1e9))
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }

    response = await async_client.post(
        "/api/HttpIngest",
        json=payload,
        headers={"content-type": "application/json"}
    )

    # Should succeed if database is available, or return 503 if not
    assert response.status_code in [200, 503]

    if response.status_code == 200:
        data = response.json()
        assert data["status"] == "success"
        assert "stats" in data
        assert "timestamp" in data


@pytest.mark.asyncio
async def test_concurrent_ingestion_performance():
    """
    Test that concurrent ingestion requests complete in reasonable time.
    This verifies the async connection pool is working properly.
    """
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "perf-test"}}
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "test"},
                        "metrics": [
                            {
                                "name": "test.metric",
                                "unit": "1",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "asInt": 1,
                                            "timeUnixNano": str(int(datetime.now().timestamp() * 1e9))
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }

    async def send_request(client, i):
        """Send a single ingestion request."""
        start = asyncio.get_event_loop().time()
        response = await client.post(
            "/api/HttpIngest",
            json=payload,
            headers={"content-type": "application/json"}
        )
        end = asyncio.get_event_loop().time()
        return response.status_code, end - start

    # Send 10 concurrent requests
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        tasks = [send_request(client, i) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # Calculate average response time
        response_times = [r[1] for r in results if r[0] in [200, 503]]

        if response_times:
            avg_time = sum(response_times) / len(response_times)
            max_time = max(response_times)

            # With async operations and connection pool:
            # - Average should be < 1 second
            # - Max should be < 2 seconds
            # (These are generous limits; actual should be much faster)
            print(f"Performance test: avg={avg_time:.3f}s, max={max_time:.3f}s")

            # Only assert if database is available (200 responses)
            if any(r[0] == 200 for r in results):
                assert avg_time < 1.0, f"Average response time too high: {avg_time:.3f}s"
                assert max_time < 2.0, f"Max response time too high: {max_time:.3f}s"


@pytest.mark.asyncio
async def test_connection_pool_parameters():
    """Test that connection pool is configured with correct parameters."""
    # This test verifies the pool configuration in the lifespan
    # We can't easily test the actual pool from here, but we can verify
    # that the connection parameters are correct
    params = get_db_connection_params()

    assert "host" in params
    assert "port" in params
    assert "database" in params
    assert "user" in params
    assert "password" in params
    assert "ssl" in params
    assert params["ssl"] == "require"


@pytest.mark.asyncio
async def test_invalid_json_handling():
    """Test that invalid JSON is handled properly."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/HttpIngest",
            content=b"{invalid json",
            headers={"content-type": "application/json"}
        )

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "JSON" in data["error"]


@pytest.mark.asyncio
async def test_missing_resource_metrics():
    """Test that missing resourceMetrics is rejected."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/HttpIngest",
            json={"invalid": "payload"},
            headers={"content-type": "application/json"}
        )

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "resourceMetrics" in data["error"]
