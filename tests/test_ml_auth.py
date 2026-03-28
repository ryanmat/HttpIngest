# Description: Tests for ML API authentication and rate limiting.
# Description: Validates API key auth on /api/ml/* endpoints and rate limit enforcement.

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def valid_api_key():
    """Return the test API key."""
    return "test-ml-api-key-12345"


@pytest.fixture
def client(valid_api_key):
    """Create a test client with ML_API_KEY set."""
    with patch.dict(os.environ, {
        "ML_API_KEY": valid_api_key,
        "HOT_CACHE_ENABLED": "false",
        "SYNAPSE_ENABLED": "false",
    }):
        # Reimport to pick up env vars before app creation
        import importlib
        import containerapp_main
        importlib.reload(containerapp_main)
        yield TestClient(containerapp_main.app)


class TestMLAPIKeyAuth:
    """Test API key authentication on ML endpoints."""

    ML_ENDPOINTS = [
        "/api/ml/inventory",
        "/api/ml/training-data",
        "/api/ml/profiles",
        "/api/ml/profile-coverage",
        "/api/ml/quality",
    ]

    def test_missing_api_key_returns_401(self, client):
        """Requests without X-API-Key header should be rejected."""
        for endpoint in self.ML_ENDPOINTS:
            response = client.get(endpoint)
            assert response.status_code == 401, f"{endpoint} should reject missing key"
            assert "Missing" in response.json().get("detail", "") or "api" in response.json().get("detail", "").lower()

    def test_wrong_api_key_returns_401(self, client):
        """Requests with incorrect API key should be rejected."""
        for endpoint in self.ML_ENDPOINTS:
            response = client.get(endpoint, headers={"X-API-Key": "wrong-key"})
            assert response.status_code == 401, f"{endpoint} should reject wrong key"

    def test_valid_api_key_passes_auth(self, client, valid_api_key):
        """Requests with correct API key should pass authentication.

        Endpoint may return 503 (no data source) but NOT 401.
        """
        for endpoint in self.ML_ENDPOINTS:
            response = client.get(endpoint, headers={"X-API-Key": valid_api_key})
            assert response.status_code != 401, f"{endpoint} should accept valid key"

    def test_non_ml_endpoints_skip_auth(self, client):
        """Non-ML endpoints should not require API key."""
        response = client.get("/api/health")
        assert response.status_code != 401

    def test_empty_api_key_returns_401(self, client):
        """Empty API key header should be rejected."""
        response = client.get("/api/ml/profiles", headers={"X-API-Key": ""})
        assert response.status_code == 401


class TestMLRateLimiting:
    """Test rate limiting on ML endpoints."""

    def test_rate_limit_returns_429_on_excess(self, client, valid_api_key):
        """Exceeding rate limit should return 429."""
        headers = {"X-API-Key": valid_api_key}
        # Hit the endpoint 15 times (limit is 10/minute)
        responses = []
        for _ in range(15):
            resp = client.get("/api/ml/profiles", headers=headers)
            responses.append(resp.status_code)

        assert 429 in responses, "Should hit rate limit after 10+ requests"

    def test_rate_limit_429_body_has_error(self, client, valid_api_key):
        """429 response body should indicate rate limit exceeded."""
        headers = {"X-API-Key": valid_api_key}
        for _ in range(15):
            resp = client.get("/api/ml/profiles", headers=headers)
            if resp.status_code == 429:
                body = resp.text.lower()
                assert "rate limit" in body or "limit" in body or "exceeded" in body
                break

    def test_normal_requests_under_limit_succeed(self, client, valid_api_key):
        """Requests under the rate limit should succeed normally."""
        headers = {"X-API-Key": valid_api_key}
        # Just 3 requests -- well under limit
        for _ in range(3):
            resp = client.get("/api/ml/profiles", headers=headers)
            assert resp.status_code != 429
