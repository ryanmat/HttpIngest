# Description: Test script to verify Data Lake ingestion pipeline end-to-end
# Description: Sends sample OTLP data and verifies it's written to Data Lake

"""
End-to-End Test for Data Lake Ingestion.

This script tests the new Data Lake ingestion pipeline by:
1. Sending sample OTLP payloads to the ingestion router
2. Verifying data is buffered correctly
3. Triggering a flush to Data Lake
4. Optionally checking the written Parquet files

Usage:
    uv run python scripts/test_datalake_ingestion.py [--flush] [--verify]
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from uuid import uuid4

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.datalake_writer import DataLakeWriter, DataLakeConfig
from src.ingestion_router import IngestionRouter, IngestionConfig
from src.otlp_parser import parse_otlp

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_sample_otlp_payload(
    num_resources: int = 2,
    num_metrics: int = 3,
    num_datapoints: int = 5,
) -> dict:
    """Generate a sample OTLP payload for testing."""
    resource_metrics = []

    for r in range(num_resources):
        resource_id = f"test-resource-{r}-{uuid4().hex[:8]}"

        # Build all metrics for this resource
        metrics_list = []
        for m in range(num_metrics):
            metric_name = f"test.metric.{m}"

            datapoints = []
            for d in range(num_datapoints):
                datapoints.append({
                    "asDouble": 42.0 + r + m + d,
                    "timeUnixNano": str(int(datetime.now(timezone.utc).timestamp() * 1e9) - d * 60_000_000_000),
                    "attributes": [
                        {"key": "instance", "value": {"stringValue": f"instance-{d}"}},
                    ],
                })

            metrics_list.append({
                "name": metric_name,
                "unit": "count",
                "description": f"Test metric {m}",
                "gauge": {
                    "dataPoints": datapoints
                }
            })

        resource_metrics.append({
            "resource": {
                "attributes": [
                    {"key": "host.name", "value": {"stringValue": f"test-host-{r}"}},
                    {"key": "service.name", "value": {"stringValue": "test-service"}},
                    {"key": "resource.id", "value": {"stringValue": resource_id}},
                ]
            },
            "scopeMetrics": [
                {
                    "scope": {
                        "name": "TestDataSource",
                        "version": "1.0.0"
                    },
                    "metrics": metrics_list
                }
            ]
        })

    return {"resourceMetrics": resource_metrics}


async def test_parsing():
    """Test OTLP parsing."""
    logger.info("=" * 60)
    logger.info("Test 1: OTLP Parsing")
    logger.info("=" * 60)

    payload = generate_sample_otlp_payload(num_resources=2, num_metrics=3, num_datapoints=5)

    logger.info(f"Generated payload with {len(payload['resourceMetrics'])} resource metrics")

    parsed = parse_otlp(payload)

    logger.info(f"Parsed results:")
    logger.info(f"  Resources: {len(parsed.resources)}")
    logger.info(f"  Datasources: {len(parsed.datasources)}")
    logger.info(f"  Metric definitions: {len(parsed.metric_definitions)}")
    logger.info(f"  Metric data points: {len(parsed.metric_data)}")

    assert len(parsed.resources) == 2, "Expected 2 resources"
    # Parser creates datasource entries per resource-scope (deduplicated later in ingestion)
    assert len(parsed.datasources) >= 1, "Expected at least 1 datasource"
    # Parser creates metric definitions per resource-scope-metric (deduplicated later)
    assert len(parsed.metric_definitions) >= 3, "Expected at least 3 metric definitions"
    assert len(parsed.metric_data) == 30, "Expected 30 data points (2 * 3 * 5)"

    logger.info("PASSED")
    return True


async def test_datalake_writer_buffering():
    """Test Data Lake writer buffering."""
    logger.info("=" * 60)
    logger.info("Test 2: Data Lake Writer Buffering")
    logger.info("=" * 60)

    config = DataLakeConfig(
        account_name="test-account",
        filesystem="test-filesystem",
        flush_threshold_rows=1000,  # High threshold so we don't auto-flush
    )
    writer = DataLakeWriter(config)

    payload = generate_sample_otlp_payload(num_resources=2, num_metrics=3, num_datapoints=5)
    parsed = parse_otlp(payload)

    # Buffer the data
    buffered = await writer.write_metrics(parsed)

    logger.info(f"Buffered {buffered} metric data points")

    stats = writer.get_buffer_stats()
    logger.info(f"Buffer stats: {stats}")

    assert stats["metric_data_buffered"] == 30, "Expected 30 buffered data points"
    assert stats["resources_buffered"] == 2, "Expected 2 buffered resources"
    assert stats["datasources_buffered"] >= 1, "Expected at least 1 buffered datasource"
    assert stats["metric_definitions_buffered"] >= 3, "Expected at least 3 buffered metric definitions"

    logger.info("PASSED")
    return True


async def test_ingestion_router_datalake_only():
    """Test ingestion router in Data Lake only mode."""
    logger.info("=" * 60)
    logger.info("Test 3: Ingestion Router (Data Lake Only)")
    logger.info("=" * 60)

    # Create Data Lake writer with test config
    datalake_config = DataLakeConfig(
        account_name="test-account",
        filesystem="test-filesystem",
        flush_threshold_rows=1000,
    )
    datalake_writer = DataLakeWriter(datalake_config)

    # Create ingestion router (no hot cache)
    ingestion_config = IngestionConfig(
        write_to_datalake=True,
        write_to_hot_cache=False,
    )
    router = IngestionRouter(
        datalake_writer=datalake_writer,
        db_pool=None,  # No PostgreSQL
        config=ingestion_config,
    )

    # Generate and ingest payload
    payload = generate_sample_otlp_payload(num_resources=3, num_metrics=2, num_datapoints=10)

    stats = await router.ingest(payload)

    logger.info(f"Ingestion stats:")
    logger.info(f"  Resources: {stats.resources}")
    logger.info(f"  Datasources: {stats.datasources}")
    logger.info(f"  Metric definitions: {stats.metric_definitions}")
    logger.info(f"  Metric data: {stats.metric_data}")
    logger.info(f"  Data Lake written (buffered): {stats.datalake_written}")
    logger.info(f"  Hot cache written: {stats.hot_cache_written}")
    logger.info(f"  Errors: {stats.errors}")

    assert stats.resources == 3, "Expected 3 resources"
    assert stats.datasources >= 1, "Expected at least 1 datasource"
    assert stats.metric_definitions >= 2, "Expected at least 2 metric definitions"
    assert stats.metric_data == 60, "Expected 60 data points (3 * 2 * 10)"
    assert stats.datalake_written == 60, "Expected 60 buffered to Data Lake"
    assert stats.hot_cache_written == 0, "Expected 0 written to hot cache"
    assert len(stats.errors) == 0, "Expected no errors"

    logger.info("PASSED")
    return True


async def test_live_datalake_flush(skip_if_no_azure: bool = True):
    """Test actual Data Lake flush (requires Azure credentials)."""
    logger.info("=" * 60)
    logger.info("Test 4: Live Data Lake Flush")
    logger.info("=" * 60)

    # Check if we have Azure credentials
    datalake_account = os.getenv("DATALAKE_ACCOUNT")
    if not datalake_account and skip_if_no_azure:
        logger.info("SKIPPED - No DATALAKE_ACCOUNT environment variable set")
        return True

    config = DataLakeConfig.from_env()
    writer = DataLakeWriter(config)

    # Generate and buffer data
    payload = generate_sample_otlp_payload(num_resources=1, num_metrics=2, num_datapoints=5)
    parsed = parse_otlp(payload)

    await writer.write_metrics(parsed)

    logger.info(f"Buffer stats before flush: {writer.get_buffer_stats()}")

    # Flush to Data Lake
    try:
        written = await writer.flush()
        logger.info(f"Flushed {written} records to Data Lake")
        logger.info(f"Buffer stats after flush: {writer.get_buffer_stats()}")
        logger.info("PASSED")
        return True
    except Exception as e:
        logger.error(f"Flush failed: {e}")
        logger.info("FAILED")
        return False


async def main():
    parser = argparse.ArgumentParser(description="Test Data Lake ingestion pipeline")
    parser.add_argument("--live", action="store_true", help="Run live Azure tests (requires credentials)")
    args = parser.parse_args()

    logger.info("Starting Data Lake Ingestion Tests")
    logger.info("=" * 60)

    results = {}

    # Run tests
    results["parsing"] = await test_parsing()
    results["buffering"] = await test_datalake_writer_buffering()
    results["router"] = await test_ingestion_router_datalake_only()

    if args.live:
        results["live_flush"] = await test_live_datalake_flush(skip_if_no_azure=False)
    else:
        logger.info("\nSkipping live Azure tests (use --live to enable)")

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    all_passed = True
    for test_name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        logger.info(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    logger.info("=" * 60)
    if all_passed:
        logger.info("ALL TESTS PASSED")
        return 0
    else:
        logger.info("SOME TESTS FAILED")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
