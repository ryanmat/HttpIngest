# Description: Async integration tests for ML data service endpoints.
# Description: Tests MLDataService against real Azure PostgreSQL database.

"""
Integration tests for ML data service using real database connections.

These tests use asyncpg to connect to the live Azure PostgreSQL database
and validate the MLDataService functionality with real data.
"""

import pytest
import pytest_asyncio
import asyncpg
import subprocess
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.ml_service import MLDataService, FEATURE_PROFILES


def get_azure_token() -> Optional[str]:
    """Get a real Azure AD token for database authentication."""
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


@pytest.fixture(scope="module")
def db_config():
    """Database configuration for tests."""
    return {
        "host": os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com'),
        "database": os.environ.get('PGDATABASE', 'postgres'),
        "user": os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com'),
        "port": int(os.environ.get('PGPORT', '5432')),
    }


@pytest.fixture(scope="module")
def azure_token():
    """Get Azure AD token for database authentication."""
    token = os.environ.get('PGPASSWORD') or get_azure_token()
    if not token:
        pytest.skip("Azure AD token not available for database testing")
    return token


@pytest_asyncio.fixture(scope="module")
async def async_pool(db_config, azure_token):
    """Create asyncpg connection pool for tests."""
    try:
        pool = await asyncpg.create_pool(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=azure_token,
            ssl="require",
            min_size=1,
            max_size=5,
            command_timeout=30,
        )
        yield pool
        await pool.close()
    except Exception as e:
        pytest.skip(f"Database connection failed: {str(e)[:100]}")


@pytest_asyncio.fixture(scope="module")
async def ml_service(async_pool):
    """Create MLDataService instance with real database pool."""
    return MLDataService(async_pool)


# ============================================================================
# MLDataService.get_inventory Tests
# ============================================================================


class TestMLInventoryIntegration:
    """Integration tests for get_inventory method."""

    async def test_inventory_returns_metrics(self, ml_service):
        """Inventory should return metric definitions from database."""
        result = await ml_service.get_inventory()

        assert result.metrics is not None
        assert isinstance(result.metrics, list)
        # Production database has metrics
        if len(result.metrics) > 0:
            first_metric = result.metrics[0]
            assert "name" in first_metric
            assert "datasource" in first_metric

    async def test_inventory_returns_resources(self, ml_service):
        """Inventory should return resources from database."""
        result = await ml_service.get_inventory()

        assert result.resources is not None
        assert isinstance(result.resources, list)
        # Production database has resources
        if len(result.resources) > 0:
            first_resource = result.resources[0]
            assert "id" in first_resource

    async def test_inventory_returns_datasources(self, ml_service):
        """Inventory should return datasources from database."""
        result = await ml_service.get_inventory()

        assert result.datasources is not None
        assert isinstance(result.datasources, list)

    async def test_inventory_returns_time_range(self, ml_service):
        """Inventory should return time range of available data."""
        result = await ml_service.get_inventory()

        assert result.time_range is not None
        assert "start" in result.time_range
        assert "end" in result.time_range

    async def test_inventory_returns_total_count(self, ml_service):
        """Inventory should return total data point count."""
        result = await ml_service.get_inventory()

        assert result.total_data_points is not None
        assert isinstance(result.total_data_points, int)
        assert result.total_data_points >= 0

    async def test_inventory_with_datasource_filter(self, ml_service):
        """Inventory should filter by datasource name."""
        # First get unfiltered to find a datasource
        unfiltered = await ml_service.get_inventory()

        if len(unfiltered.datasources) > 0:
            datasource_name = unfiltered.datasources[0].get("name")
            if datasource_name:
                filtered = await ml_service.get_inventory(datasource=datasource_name)
                # Filtered results should have fewer or equal metrics
                assert len(filtered.metrics) <= len(unfiltered.metrics)


# ============================================================================
# MLDataService.get_training_data Tests
# ============================================================================


class TestMLTrainingDataIntegration:
    """Integration tests for get_training_data method."""

    async def test_training_data_returns_records(self, ml_service):
        """Training data should return metric records."""
        result = await ml_service.get_training_data(limit=100)

        assert "data" in result
        assert "meta" in result
        assert isinstance(result["data"], list)

    async def test_training_data_record_structure(self, ml_service):
        """Each training data record should have required fields."""
        result = await ml_service.get_training_data(limit=10)

        if len(result["data"]) > 0:
            record = result["data"][0]
            assert "resource_id" in record
            assert "metric_name" in record
            assert "timestamp" in record
            assert "value" in record
            assert "datasource_name" in record

    async def test_training_data_with_profile_filter(self, ml_service):
        """Training data should filter by profile."""
        result = await ml_service.get_training_data(profile="collector", limit=100)

        assert result["meta"]["profile"] == "collector"
        # All metrics should be in the collector profile
        collector_features = (
            FEATURE_PROFILES["collector"]["numerical_features"] +
            FEATURE_PROFILES["collector"]["categorical_features"]
        )
        for record in result["data"]:
            assert record["metric_name"] in collector_features

    async def test_training_data_respects_limit(self, ml_service):
        """Training data should respect limit parameter."""
        result = await ml_service.get_training_data(limit=5)

        assert len(result["data"]) <= 5
        assert result["meta"]["limit"] == 5

    async def test_training_data_time_range(self, ml_service):
        """Training data should filter by time range."""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        result = await ml_service.get_training_data(
            start_time=start_time,
            end_time=end_time,
            limit=100
        )

        assert result["meta"]["start_time"] == start_time.isoformat()
        assert result["meta"]["end_time"] == end_time.isoformat()

        # All records should be within time range
        for record in result["data"]:
            ts = datetime.fromisoformat(record["timestamp"])
            assert start_time <= ts <= end_time

    async def test_training_data_meta_includes_total(self, ml_service):
        """Training data meta should include total record count."""
        result = await ml_service.get_training_data(limit=10)

        assert "total" in result["meta"]
        assert isinstance(result["meta"]["total"], int)


# ============================================================================
# MLDataService.get_profile_coverage Tests
# ============================================================================


class TestMLProfileCoverageIntegration:
    """Integration tests for get_profile_coverage method."""

    async def test_profile_coverage_returns_all_profiles(self, ml_service):
        """Coverage should return all defined profiles."""
        result = await ml_service.get_profile_coverage()

        assert "profiles" in result
        assert len(result["profiles"]) == len(FEATURE_PROFILES)

        profile_names = {p["name"] for p in result["profiles"]}
        for expected_name in FEATURE_PROFILES.keys():
            assert expected_name in profile_names

    async def test_profile_coverage_structure(self, ml_service):
        """Each profile coverage should have required fields."""
        result = await ml_service.get_profile_coverage()

        for profile in result["profiles"]:
            assert "name" in profile
            assert "description" in profile
            assert "coverage_percent" in profile
            assert "available" in profile
            assert "missing" in profile
            assert "total_expected" in profile
            assert "total_available" in profile

    async def test_profile_coverage_single_profile(self, ml_service):
        """Coverage should filter to single profile."""
        result = await ml_service.get_profile_coverage(profile="kubernetes")

        assert len(result["profiles"]) == 1
        assert result["profiles"][0]["name"] == "kubernetes"

    async def test_profile_coverage_percentages_valid(self, ml_service):
        """Coverage percentages should be between 0 and 100."""
        result = await ml_service.get_profile_coverage()

        for profile in result["profiles"]:
            assert 0 <= profile["coverage_percent"] <= 100

    async def test_profile_coverage_counts_match(self, ml_service):
        """Available + missing should equal total expected."""
        result = await ml_service.get_profile_coverage()

        for profile in result["profiles"]:
            assert len(profile["available"]) + len(profile["missing"]) == profile["total_expected"]
            assert len(profile["available"]) == profile["total_available"]


# ============================================================================
# MLDataService.get_data_quality Tests
# ============================================================================


class TestMLDataQualityIntegration:
    """Integration tests for get_data_quality method."""

    async def test_data_quality_returns_summary(self, ml_service):
        """Quality check should return summary metrics."""
        result = await ml_service.get_data_quality()

        assert "summary" in result
        summary = result["summary"]
        assert "overall_score" in summary
        assert "freshness_score" in summary
        assert "gap_score" in summary
        assert "coverage_score" in summary

    async def test_data_quality_returns_freshness(self, ml_service):
        """Quality check should return freshness data."""
        result = await ml_service.get_data_quality()

        assert "freshness" in result
        assert isinstance(result["freshness"], list)

        if len(result["freshness"]) > 0:
            freshness_item = result["freshness"][0]
            assert "resource_id" in freshness_item
            assert "last_update" in freshness_item
            assert "age_minutes" in freshness_item
            assert "is_stale" in freshness_item

    async def test_data_quality_returns_gaps(self, ml_service):
        """Quality check should return gap detection data."""
        result = await ml_service.get_data_quality()

        assert "gaps" in result
        assert isinstance(result["gaps"], list)

        if len(result["gaps"]) > 0:
            gap = result["gaps"][0]
            assert "resource_id" in gap
            assert "gap_start" in gap
            assert "gap_end" in gap
            assert "gap_minutes" in gap

    async def test_data_quality_returns_ranges(self, ml_service):
        """Quality check should return value range statistics."""
        result = await ml_service.get_data_quality()

        assert "ranges" in result
        assert isinstance(result["ranges"], list)

        if len(result["ranges"]) > 0:
            range_item = result["ranges"][0]
            assert "metric_name" in range_item
            assert "sample_count" in range_item
            assert "avg_value" in range_item

    async def test_data_quality_with_profile_filter(self, ml_service):
        """Quality check should filter by profile."""
        result = await ml_service.get_data_quality(profile="collector")

        assert result["summary"]["profile"] == "collector"

    async def test_data_quality_with_hours_parameter(self, ml_service):
        """Quality check should respect hours lookback."""
        result = await ml_service.get_data_quality(hours=12)

        assert result["summary"]["lookback_hours"] == 12

    async def test_data_quality_scores_valid(self, ml_service):
        """Quality scores should be between 0 and 100."""
        result = await ml_service.get_data_quality()

        summary = result["summary"]
        assert 0 <= summary["overall_score"] <= 100
        assert 0 <= summary["freshness_score"] <= 100
        assert 0 <= summary["gap_score"] <= 100
        assert 0 <= summary["coverage_score"] <= 100

    async def test_data_quality_json_serializable(self, ml_service):
        """Quality results should be JSON serializable (no NaN/Inf/Decimal)."""
        import json

        result = await ml_service.get_data_quality()

        # This should not raise
        json_str = json.dumps(result)
        assert json_str is not None
        assert "NaN" not in json_str
        assert "Infinity" not in json_str


# ============================================================================
# Cross-Method Integration Tests
# ============================================================================


class TestMLServiceIntegration:
    """Integration tests that verify cross-method consistency."""

    async def test_inventory_and_training_data_consistency(self, ml_service):
        """Metrics in training data should exist in inventory."""
        inventory = await ml_service.get_inventory()
        training = await ml_service.get_training_data(limit=100)

        inventory_metrics = {m["name"] for m in inventory.metrics}

        for record in training["data"]:
            # Training data metric names should be in inventory
            # (unless database changed between calls)
            assert record["metric_name"] in inventory_metrics or len(inventory_metrics) == 0

    async def test_profile_coverage_and_training_data_consistency(self, ml_service):
        """Metrics returned for a profile should match coverage."""
        coverage = await ml_service.get_profile_coverage(profile="collector")
        training = await ml_service.get_training_data(profile="collector", limit=100)

        available_metrics = set(coverage["profiles"][0]["available"])

        for record in training["data"]:
            # All training data metrics should be in available set
            assert record["metric_name"] in available_metrics

    async def test_quality_and_freshness_consistency(self, ml_service):
        """Quality freshness data should match actual data state."""
        quality = await ml_service.get_data_quality(hours=24)

        # If we have resources with data points, stale count should be <= total
        summary = quality["summary"]
        assert summary["stale_resources"] <= summary["total_resources"]

        # Freshness list should not exceed total resources
        assert len(quality["freshness"]) <= summary["total_resources"]


# ============================================================================
# Performance Tests
# ============================================================================


class TestMLPerformance:
    """Performance tests for ML data service."""

    async def test_inventory_performance(self, ml_service):
        """Inventory query should complete within reasonable time."""
        import time

        start = time.time()
        await ml_service.get_inventory()
        elapsed = time.time() - start

        # Should complete in under 5 seconds
        assert elapsed < 5.0, f"Inventory took {elapsed:.2f}s, expected < 5s"

    async def test_training_data_performance(self, ml_service):
        """Training data query should complete within reasonable time."""
        import time

        start = time.time()
        await ml_service.get_training_data(limit=1000)
        elapsed = time.time() - start

        # Should complete in under 10 seconds for 1000 records
        assert elapsed < 10.0, f"Training data took {elapsed:.2f}s, expected < 10s"

    async def test_profile_coverage_performance(self, ml_service):
        """Profile coverage query should complete within reasonable time."""
        import time

        start = time.time()
        await ml_service.get_profile_coverage()
        elapsed = time.time() - start

        # Should complete in under 3 seconds
        assert elapsed < 3.0, f"Profile coverage took {elapsed:.2f}s, expected < 3s"

    async def test_data_quality_performance(self, ml_service):
        """Data quality query should complete within reasonable time."""
        import time

        start = time.time()
        await ml_service.get_data_quality(hours=24)
        elapsed = time.time() - start

        # Should complete in under 15 seconds
        assert elapsed < 15.0, f"Data quality took {elapsed:.2f}s, expected < 15s"
