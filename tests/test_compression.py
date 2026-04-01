# Description: Tests for GZipMiddleware response compression
# Description: Validates middleware registration and minimum size config

"""
Tests for GZipMiddleware integration on the FastAPI application.
"""

import pytest
from starlette.middleware.gzip import GZipMiddleware


class TestGZipMiddlewareConfig:
    """Tests for GZipMiddleware registration and configuration."""

    def test_gzip_middleware_registered(self):
        """GZipMiddleware should be configured on the application."""
        import containerapp_main

        middleware_types = [m.cls for m in containerapp_main.app.user_middleware]
        assert GZipMiddleware in middleware_types

    def test_gzip_minimum_size_is_1000(self):
        """GZipMiddleware should compress responses larger than 1000 bytes."""
        import containerapp_main

        gzip_mw = next(
            m for m in containerapp_main.app.user_middleware if m.cls is GZipMiddleware
        )
        assert gzip_mw.kwargs.get("minimum_size") == 1000


class TestEndpointsWithCompression:
    """Tests that endpoints work correctly with GZipMiddleware enabled."""

    @pytest.fixture
    def client(self):
        """Create test client with compression-enabled app."""
        from fastapi.testclient import TestClient

        import containerapp_main

        with TestClient(containerapp_main.app, raise_server_exceptions=False) as c:
            yield c

    def test_health_endpoint_works(self, client):
        """Health endpoint should work with compression middleware."""
        response = client.get("/health")

        # May return 503 if datalake_writer is None in test environment
        assert response.status_code in [200, 503]
        data = response.json()
        assert "status" in data
