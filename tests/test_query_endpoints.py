# ABOUTME: Tests for query endpoints including /api/metrics, /api/resources, /api/timeseries, /api/aggregates
# ABOUTME: Tests pagination, filtering, sorting, and OpenAPI documentation

import pytest
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, MagicMock, patch

from src.function_app import (
    get_metrics,
    get_resources,
    get_timeseries,
    get_aggregates,
    api_docs,
    parse_pagination_params,
    build_paginated_response
)


class TestPaginationUtilities:
    """Tests for pagination helper functions."""

    def test_parse_pagination_params_defaults(self):
        """Test pagination params with defaults."""
        mock_req = Mock()
        mock_req.params = {}

        result = parse_pagination_params(mock_req)

        assert result["page"] == 1
        assert result["page_size"] == 100
        assert result["offset"] == 0
        assert result["limit"] == 100

    def test_parse_pagination_params_custom(self):
        """Test pagination params with custom values."""
        mock_req = Mock()
        mock_req.params = {"page": "3", "page_size": "50"}

        result = parse_pagination_params(mock_req)

        assert result["page"] == 3
        assert result["page_size"] == 50
        assert result["offset"] == 100  # (3-1) * 50
        assert result["limit"] == 50

    def test_parse_pagination_params_max_page_size(self):
        """Test pagination enforces max page size."""
        mock_req = Mock()
        mock_req.params = {"page_size": "5000"}

        result = parse_pagination_params(mock_req)

        assert result["page_size"] == 1000  # Max enforced

    def test_parse_pagination_params_invalid(self):
        """Test pagination with invalid values returns defaults."""
        mock_req = Mock()
        mock_req.params = {"page": "invalid", "page_size": "bad"}

        result = parse_pagination_params(mock_req)

        assert result["page"] == 1
        assert result["page_size"] == 100

    def test_build_paginated_response(self):
        """Test building paginated response structure."""
        data = [{"id": 1}, {"id": 2}]
        total_count = 25
        page = 2
        page_size = 10

        result = build_paginated_response(data, total_count, page, page_size)

        assert result["data"] == data
        assert result["pagination"]["page"] == 2
        assert result["pagination"]["page_size"] == 10
        assert result["pagination"]["total_items"] == 25
        assert result["pagination"]["total_pages"] == 3
        assert result["pagination"]["has_next"] is True
        assert result["pagination"]["has_prev"] is True
        assert "timestamp" in result


class TestMetricsEndpoint:
    """Tests for GET /api/metrics endpoint."""

    @patch('src.function_app.get_db_connection')
    def test_get_metrics_basic(self, mock_get_conn, db_connection):
        """Test basic metrics endpoint without filters."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('TestDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description, created_at)
                VALUES (%s, 'test.metric', 'count', 'gauge', 'Test metric', NOW())
            """, (ds_id,))

            db_connection.commit()

        # Create mock request
        mock_req = Mock()
        mock_req.params = {}

        # Call endpoint
        response = get_metrics(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert "data" in data
        assert "pagination" in data
        assert len(data["data"]) > 0
        assert data["data"][0]["name"] == "test.metric"

    @patch('src.function_app.get_db_connection')
    def test_get_metrics_with_filtering(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test metrics endpoint with datasource filter."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('FilteredDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description, created_at)
                VALUES (%s, 'filtered.metric', 'count', 'gauge', 'Filtered metric', NOW())
            """, (ds_id,))

            db_connection.commit()

        # Create mock request with filter
        mock_req = Mock()
        mock_req.params = {"datasource": "FilteredDS"}

        # Call endpoint
        response = get_metrics(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert len(data["data"]) > 0
        assert data["data"][0]["datasource"]["name"] == "FilteredDS"

    @patch('src.function_app.get_db_connection')
    def test_get_metrics_with_search(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test metrics endpoint with search term."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('SearchDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description, created_at)
                VALUES (%s, 'cpu.usage', 'percent', 'gauge', 'CPU usage metric', NOW())
            """, (ds_id,))

            db_connection.commit()

        # Create mock request with search
        mock_req = Mock()
        mock_req.params = {"search": "cpu"}

        # Call endpoint
        response = get_metrics(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert len(data["data"]) > 0
        assert "cpu" in data["data"][0]["name"].lower()

    @patch('src.function_app.get_db_connection')
    def test_get_metrics_pagination(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test metrics endpoint pagination."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('PaginationDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            # Insert multiple metrics
            for i in range(5):
                cur.execute("""
                    INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description, created_at)
                    VALUES (%s, %s, 'count', 'gauge', 'Test metric', NOW())
                """, (ds_id, f'metric.{i}'))

            db_connection.commit()

        # Create mock request with pagination
        mock_req = Mock()
        mock_req.params = {"page": "1", "page_size": "2"}

        # Call endpoint
        response = get_metrics(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert len(data["data"]) == 2
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == 2
        assert data["pagination"]["has_next"] is True

    @patch('src.function_app.get_db_connection')
    def test_get_metrics_sorting(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test metrics endpoint sorting."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('SortDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            # Insert metrics in specific order
            for name in ['zebra.metric', 'alpha.metric', 'beta.metric']:
                cur.execute("""
                    INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description, created_at)
                    VALUES (%s, %s, 'count', 'gauge', 'Test metric', NOW())
                """, (ds_id, name))

            db_connection.commit()

        # Create mock request with sorting
        mock_req = Mock()
        mock_req.params = {"sort": "name", "order": "asc"}

        # Call endpoint
        response = get_metrics(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        names = [m["name"] for m in data["data"]]
        assert names == sorted(names)  # Should be alphabetically sorted


class TestResourcesEndpoint:
    """Tests for GET /api/resources endpoint."""

    @patch('src.function_app.get_db_connection')
    def test_get_resources_basic(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test basic resources endpoint."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
                VALUES ('test_hash', '{"service.name": "test-service"}', NOW(), NOW())
            """)
            db_connection.commit()

        # Create mock request
        mock_req = Mock()
        mock_req.params = {}

        # Call endpoint
        response = get_resources(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert "data" in data
        assert "pagination" in data
        assert len(data["data"]) > 0
        assert data["data"][0]["resource_hash"] == "test_hash"

    @patch('src.function_app.get_db_connection')
    def test_get_resources_with_service_filter(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test resources endpoint with service name filter."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            cur.execute("""
                INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
                VALUES ('service_hash', '{"service.name": "web-server"}', NOW(), NOW())
            """)
            db_connection.commit()

        # Create mock request with filter
        mock_req = Mock()
        mock_req.params = {"service_name": "web"}

        # Call endpoint
        response = get_resources(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert len(data["data"]) > 0
        assert "web" in data["data"][0]["attributes"]["service.name"]

    @patch('src.function_app.get_db_connection')
    def test_get_resources_with_min_metrics(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test resources endpoint with minimum metrics filter."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Create mock request with min_metrics filter
        mock_req = Mock()
        mock_req.params = {"min_metrics": "1"}

        # Call endpoint
        response = get_resources(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        # All returned resources should have at least 1 metric
        for resource in data["data"]:
            assert resource["metrics"]["unique_metrics"] >= 1


class TestTimeseriesEndpoint:
    """Tests for GET /api/timeseries endpoint."""

    @patch('src.function_app.get_db_connection')
    def test_get_timeseries_missing_params(self, mock_get_conn):
        """Test timeseries endpoint with missing required parameters."""
        # Create mock request without required params
        mock_req = Mock()
        mock_req.params = {}

        # Call endpoint
        response = get_timeseries(mock_req)

        assert response.status_code == 400
        data = json.loads(response.get_body())
        assert "error" in data
        assert "Missing required parameters" in data["error"]

    @patch('src.function_app.get_db_connection')
    def test_get_timeseries_with_data(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test timeseries endpoint with valid data."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            # Create datasource
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('TimeseriesDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            # Create resource
            cur.execute("""
                INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
                VALUES ('ts_hash', '{"service.name": "ts-service"}', NOW(), NOW())
                RETURNING id
            """)
            resource_id = cur.fetchone()[0]

            # Create metric definition
            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
                VALUES (%s, 'ts.metric', 'count', 'gauge', 'Timeseries metric')
                RETURNING id
            """, (ds_id,))
            metric_def_id = cur.fetchone()[0]

            # Insert data points
            now = datetime.now(timezone.utc)
            for i in range(5):
                timestamp = now - timedelta(minutes=i)
                cur.execute("""
                    INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double, attributes, created_at)
                    VALUES (%s, %s, %s, %s, '{}', NOW())
                """, (resource_id, metric_def_id, timestamp, 50.0 + i))

            db_connection.commit()

        # Create mock request
        start_time = (now - timedelta(hours=1)).isoformat()
        end_time = now.isoformat()

        mock_req = Mock()
        mock_req.params = {
            "metric_name": "ts.metric",
            "start_time": start_time,
            "end_time": end_time
        }

        # Call endpoint
        response = get_timeseries(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert "data" in data
        assert "pagination" in data
        assert "query" in data
        assert len(data["data"]) == 5
        assert data["query"]["metric_name"] == "ts.metric"

    @patch('src.function_app.get_db_connection')
    def test_get_timeseries_with_resource_filter(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test timeseries endpoint with resource hash filter."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            # Create datasource
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('FilteredTS_DS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            # Create resource
            cur.execute("""
                INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
                VALUES ('filtered_ts_hash', '{"service.name": "filtered"}', NOW(), NOW())
                RETURNING id
            """)
            resource_id = cur.fetchone()[0]

            # Create metric definition
            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
                VALUES (%s, 'filtered.ts.metric', 'count', 'gauge', 'Filtered TS')
                RETURNING id
            """, (ds_id,))
            metric_def_id = cur.fetchone()[0]

            # Insert data point
            now = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double, attributes, created_at)
                VALUES (%s, %s, %s, %s, '{}', NOW())
            """, (resource_id, metric_def_id, now, 100.0))

            db_connection.commit()

        # Create mock request with resource filter
        start_time = (now - timedelta(hours=1)).isoformat()
        end_time = (now + timedelta(hours=1)).isoformat()

        mock_req = Mock()
        mock_req.params = {
            "metric_name": "filtered.ts.metric",
            "start_time": start_time,
            "end_time": end_time,
            "resource_hash": "filtered_ts_hash"
        }

        # Call endpoint
        response = get_timeseries(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert len(data["data"]) > 0
        assert data["data"][0]["resource"]["resource_hash"] == "filtered_ts_hash"


class TestAggregatesEndpoint:
    """Tests for GET /api/aggregates endpoint."""

    @patch('src.function_app.get_db_connection')
    def test_get_aggregates_missing_params(self, mock_get_conn):
        """Test aggregates endpoint with missing required parameters."""
        # Create mock request without required params
        mock_req = Mock()
        mock_req.params = {}

        # Call endpoint
        response = get_aggregates(mock_req)

        assert response.status_code == 400
        data = json.loads(response.get_body())
        assert "error" in data
        assert "Missing required parameters" in data["error"]

    @patch('src.function_app.get_db_connection')
    def test_get_aggregates_invalid_bucket(self, mock_get_conn):
        """Test aggregates endpoint with invalid bucket size."""
        now = datetime.now(timezone.utc)

        # Create mock request with invalid bucket
        mock_req = Mock()
        mock_req.params = {
            "metric_name": "test.metric",
            "start_time": (now - timedelta(hours=1)).isoformat(),
            "end_time": now.isoformat(),
            "bucket": "invalid_bucket"
        }

        # Call endpoint
        response = get_aggregates(mock_req)

        assert response.status_code == 400
        data = json.loads(response.get_body())
        assert "error" in data
        assert "Invalid bucket size" in data["error"]

    @patch('src.function_app.get_db_connection')
    def test_get_aggregates_with_data(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test aggregates endpoint with valid data."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            # Create datasource
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('AggregatesDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            # Create resource
            cur.execute("""
                INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
                VALUES ('agg_hash', '{"service.name": "agg-service"}', NOW(), NOW())
                RETURNING id
            """)
            resource_id = cur.fetchone()[0]

            # Create metric definition
            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
                VALUES (%s, 'agg.metric', 'count', 'gauge', 'Aggregates metric')
                RETURNING id
            """, (ds_id,))
            metric_def_id = cur.fetchone()[0]

            # Insert data points across 2 hours
            now = datetime.now(timezone.utc)
            for i in range(10):
                timestamp = now - timedelta(minutes=i * 10)
                cur.execute("""
                    INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double, attributes, created_at)
                    VALUES (%s, %s, %s, %s, '{}', NOW())
                """, (resource_id, metric_def_id, timestamp, 50.0 + i))

            db_connection.commit()

        # Create mock request
        start_time = (now - timedelta(hours=2)).isoformat()
        end_time = now.isoformat()

        mock_req = Mock()
        mock_req.params = {
            "metric_name": "agg.metric",
            "start_time": start_time,
            "end_time": end_time,
            "bucket": "1h"
        }

        # Call endpoint
        response = get_aggregates(mock_req)

        assert response.status_code == 200
        data = json.loads(response.get_body())
        assert "data" in data
        assert "pagination" in data
        assert "query" in data
        assert len(data["data"]) > 0

        # Verify aggregate stats structure
        first_bucket = data["data"][0]
        assert "time_bucket" in first_bucket
        assert "stats" in first_bucket
        assert "count" in first_bucket["stats"]
        assert "min" in first_bucket["stats"]
        assert "max" in first_bucket["stats"]
        assert "avg" in first_bucket["stats"]

    @patch('src.function_app.get_db_connection')
    def test_get_aggregates_different_buckets(self, mock_get_conn, db_connection, clean_normalized_tables):
        """Test aggregates endpoint with different bucket sizes."""
        mock_get_conn.return_value.__enter__.return_value = db_connection

        # Setup test data
        with db_connection.cursor() as cur:
            # Create datasource
            cur.execute("""
                INSERT INTO datasources (name, version, created_at)
                VALUES ('BucketDS', '1.0', NOW())
                RETURNING id
            """)
            ds_id = cur.fetchone()[0]

            # Create resource
            cur.execute("""
                INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
                VALUES ('bucket_hash', '{"service.name": "bucket-service"}', NOW(), NOW())
                RETURNING id
            """)
            resource_id = cur.fetchone()[0]

            # Create metric definition
            cur.execute("""
                INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
                VALUES (%s, 'bucket.metric', 'count', 'gauge', 'Bucket metric')
                RETURNING id
            """, (ds_id,))
            metric_def_id = cur.fetchone()[0]

            # Insert data points
            now = datetime.now(timezone.utc)
            for i in range(20):
                timestamp = now - timedelta(minutes=i * 5)
                cur.execute("""
                    INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double, attributes, created_at)
                    VALUES (%s, %s, %s, %s, '{}', NOW())
                """, (resource_id, metric_def_id, timestamp, 50.0 + i))

            db_connection.commit()

        start_time = (now - timedelta(hours=2)).isoformat()
        end_time = now.isoformat()

        # Test with different bucket sizes
        for bucket_size in ['5m', '15m', '1h']:
            mock_req = Mock()
            mock_req.params = {
                "metric_name": "bucket.metric",
                "start_time": start_time,
                "end_time": end_time,
                "bucket": bucket_size
            }

            response = get_aggregates(mock_req)

            assert response.status_code == 200
            data = json.loads(response.get_body())
            assert data["query"]["bucket"] == bucket_size


class TestOpenAPIEndpoint:
    """Tests for GET /api/docs endpoint."""

    def test_get_openapi_docs(self):
        """Test OpenAPI documentation endpoint."""
        # Create mock request
        mock_req = Mock()

        # Call endpoint
        response = api_docs(mock_req)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"

        # Parse and validate OpenAPI spec
        spec = json.loads(response.get_body())

        assert spec["openapi"] == "3.0.0"
        assert "info" in spec
        assert spec["info"]["title"] == "LogicMonitor HTTP Ingest API"
        assert spec["info"]["version"] == "10.0"

        # Verify all endpoints are documented
        assert "/health" in spec["paths"]
        assert "/metrics" in spec["paths"]
        assert "/resources" in spec["paths"]
        assert "/timeseries" in spec["paths"]
        assert "/aggregates" in spec["paths"]

        # Verify components/schemas exist
        assert "components" in spec
        assert "schemas" in spec["components"]
        assert "Metric" in spec["components"]["schemas"]
        assert "Pagination" in spec["components"]["schemas"]


class TestEndpointErrorHandling:
    """Tests for error handling across all endpoints."""

    @patch('src.function_app.get_db_connection')
    def test_metrics_endpoint_database_error(self, mock_get_conn):
        """Test metrics endpoint handles database errors gracefully."""
        # Mock database connection to raise an error
        mock_get_conn.side_effect = Exception("Database connection failed")

        mock_req = Mock()
        mock_req.params = {}

        response = get_metrics(mock_req)

        assert response.status_code == 500
        data = json.loads(response.get_body())
        assert "error" in data

    @patch('src.function_app.get_db_connection')
    def test_resources_endpoint_database_error(self, mock_get_conn):
        """Test resources endpoint handles database errors gracefully."""
        mock_get_conn.side_effect = Exception("Database connection failed")

        mock_req = Mock()
        mock_req.params = {}

        response = get_resources(mock_req)

        assert response.status_code == 500
        data = json.loads(response.get_body())
        assert "error" in data

    @patch('src.function_app.get_db_connection')
    def test_timeseries_endpoint_database_error(self, mock_get_conn):
        """Test timeseries endpoint handles database errors gracefully."""
        mock_get_conn.side_effect = Exception("Database connection failed")

        now = datetime.now(timezone.utc)
        mock_req = Mock()
        mock_req.params = {
            "metric_name": "test.metric",
            "start_time": (now - timedelta(hours=1)).isoformat(),
            "end_time": now.isoformat()
        }

        response = get_timeseries(mock_req)

        assert response.status_code == 500
        data = json.loads(response.get_body())
        assert "error" in data

    @patch('src.function_app.get_db_connection')
    def test_aggregates_endpoint_database_error(self, mock_get_conn):
        """Test aggregates endpoint handles database errors gracefully."""
        mock_get_conn.side_effect = Exception("Database connection failed")

        now = datetime.now(timezone.utc)
        mock_req = Mock()
        mock_req.params = {
            "metric_name": "test.metric",
            "start_time": (now - timedelta(hours=1)).isoformat(),
            "end_time": now.isoformat(),
            "bucket": "1h"
        }

        response = get_aggregates(mock_req)

        assert response.status_code == 500
        data = json.loads(response.get_body())
        assert "error" in data
