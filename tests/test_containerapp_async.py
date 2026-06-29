# Description: Tests for async containerapp endpoints
# Description: Verifies health check, ingestion, error handling, and bearer auth

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from containerapp_main import app

TOKEN = "test-bearer-token-do-not-use-in-prod"
INGEST_HEADERS = {
    "content-type": "application/json",
    "authorization": f"Bearer {TOKEN}",
}


@pytest.fixture(autouse=True)
def _set_token(monkeypatch):
    """Default every test in this module to a known good bearer token."""
    monkeypatch.setenv("INGEST_BEARER_TOKEN", TOKEN)


@pytest_asyncio.fixture
async def async_client():
    """Create an async test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _sample_payload(service: str = "test-service", value: int = 42) -> dict:
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}},
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
                                            "asInt": value,
                                            "timeUnixNano": str(
                                                int(datetime.now().timestamp() * 1e9)
                                            ),
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


@pytest.mark.asyncio
async def test_health_check_async():
    """The detailed health endpoint reports component status (bearer-gated)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # /api/health is bearer-gated; leaks buffer sizes + task names if open.
        unauth = await client.get("/api/health")
        assert unauth.status_code == 401

        response = await client.get("/api/health", headers=INGEST_HEADERS)
        assert response.status_code in [200, 503]
        data = response.json()

        assert "status" in data
        assert "timestamp" in data
        assert "components" in data
        assert "datalake" in data["components"]
        assert "ingestion_router" in data["components"]
        assert "background_tasks" in data["components"]


@pytest.mark.asyncio
async def test_http_ingest_async(async_client):
    """The ingestion endpoint accepts an authenticated OTLP payload."""
    response = await async_client.post(
        "/api/HttpIngest", json=_sample_payload(), headers=INGEST_HEADERS
    )

    assert response.status_code in [200, 503]

    if response.status_code == 200:
        data = response.json()
        assert data["status"] == "success"
        assert "stats" in data
        assert "timestamp" in data


@pytest.mark.asyncio
async def test_ingest_rejects_missing_authorization(async_client):
    """No Authorization header returns 401."""
    response = await async_client.post(
        "/api/HttpIngest",
        json=_sample_payload(),
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 401
    assert "Authorization" in response.json()["detail"]


@pytest.mark.asyncio
async def test_ingest_rejects_non_bearer_authorization(async_client):
    """A non-Bearer Authorization scheme returns 401."""
    response = await async_client.post(
        "/api/HttpIngest",
        json=_sample_payload(),
        headers={"content-type": "application/json", "authorization": "Basic dXNlcjpwYXNz"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_ingest_rejects_wrong_bearer_token(async_client):
    """A Bearer scheme with the wrong token returns 401."""
    response = await async_client.post(
        "/api/HttpIngest",
        json=_sample_payload(),
        headers={"content-type": "application/json", "authorization": "Bearer wrong"},
    )
    assert response.status_code == 401
    assert "Invalid bearer token" in response.json()["detail"]


@pytest.mark.asyncio
async def test_ingest_503_when_auth_unconfigured(async_client, monkeypatch):
    """If INGEST_BEARER_TOKEN is unset on the server, every ingest returns 503."""
    monkeypatch.delenv("INGEST_BEARER_TOKEN", raising=False)
    response = await async_client.post(
        "/api/HttpIngest",
        json=_sample_payload(),
        headers=INGEST_HEADERS,
    )
    assert response.status_code == 503
    assert "INGEST_BEARER_TOKEN" in response.json()["detail"]


@pytest.mark.asyncio
async def test_concurrent_ingestion_performance():
    """Concurrent authenticated ingest requests complete in reasonable time."""

    async def send_request(client):
        start = asyncio.get_event_loop().time()
        response = await client.post(
            "/api/HttpIngest",
            json=_sample_payload(service="perf-test", value=1),
            headers=INGEST_HEADERS,
        )
        end = asyncio.get_event_loop().time()
        return response.status_code, end - start

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        tasks = [send_request(client) for _ in range(10)]
        results = await asyncio.gather(*tasks)

        response_times = [r[1] for r in results if r[0] in [200, 503]]

        if response_times:
            avg_time = sum(response_times) / len(response_times)
            max_time = max(response_times)

            print(f"Performance test: avg={avg_time:.3f}s, max={max_time:.3f}s")

            if any(r[0] == 200 for r in results):
                assert avg_time < 1.0, f"Average response time too high: {avg_time:.3f}s"
                assert max_time < 2.0, f"Max response time too high: {max_time:.3f}s"


@pytest.mark.asyncio
async def test_invalid_json_handling():
    """Invalid JSON is rejected with 400 (after auth passes)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/HttpIngest",
            content=b"{invalid json",
            headers=INGEST_HEADERS,
        )

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "JSON" in data["error"]


@pytest.mark.asyncio
async def test_missing_resource_metrics():
    """Payload without resourceMetrics is rejected with 400."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/HttpIngest",
            json={"invalid": "payload"},
            headers=INGEST_HEADERS,
        )

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "resourceMetrics" in data["error"]


def test_module_exposes_verify_bearer_token():
    """Sanity check: the auth dependency is importable."""
    from containerapp_main import verify_bearer_token

    assert callable(verify_bearer_token)
