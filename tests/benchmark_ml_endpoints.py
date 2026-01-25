# Description: Performance benchmark for ML data service endpoints.
# Description: Measures query times against production database with ~441K rows.

"""
Performance benchmark for ML endpoints against production data.

Run with: uv run python tests/benchmark_ml_endpoints.py
"""

import asyncio
import asyncpg
import subprocess
import os
import sys
import time
import statistics
from datetime import datetime, timedelta, timezone

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ml_service import MLDataService


def get_azure_token():
    """Get Azure AD token."""
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


async def benchmark():
    """Run benchmarks against production database."""
    token = os.environ.get('PGPASSWORD') or get_azure_token()
    if not token:
        print("ERROR: Could not get Azure AD token")
        return

    pool = await asyncpg.create_pool(
        host='rm-postgres.postgres.database.azure.com',
        port=5432,
        database='postgres',
        user='ryan.matuszewski@logicmonitor.com',
        password=token,
        ssl='require',
        min_size=1,
        max_size=5,
    )

    service = MLDataService(pool)
    iterations = 5

    print("=" * 60)
    print("ML Endpoints Performance Benchmark")
    print("=" * 60)

    # Get database stats first
    async with pool.acquire() as conn:
        row_count = await conn.fetchval("SELECT COUNT(*) FROM metric_data")
        partition_count = await conn.fetchval("""
            SELECT COUNT(*) FROM pg_tables
            WHERE tablename LIKE 'metric_data_%'
        """)
    print(f"Database: {row_count:,} rows in metric_data ({partition_count} partitions)")
    print("-" * 60)

    # Benchmark get_inventory
    times = []
    for i in range(iterations):
        start = time.time()
        result = await service.get_inventory()
        elapsed = time.time() - start
        times.append(elapsed)
    print(f"get_inventory():")
    print(f"  Mean: {statistics.mean(times)*1000:.1f}ms")
    print(f"  Min:  {min(times)*1000:.1f}ms")
    print(f"  Max:  {max(times)*1000:.1f}ms")
    print(f"  Metrics: {len(result.metrics)}, Resources: {len(result.resources)}")
    print()

    # Benchmark get_training_data with various limits
    for limit in [100, 1000, 10000]:
        times = []
        for i in range(iterations):
            start = time.time()
            result = await service.get_training_data(limit=limit)
            elapsed = time.time() - start
            times.append(elapsed)
        print(f"get_training_data(limit={limit}):")
        print(f"  Mean: {statistics.mean(times)*1000:.1f}ms")
        print(f"  Min:  {min(times)*1000:.1f}ms")
        print(f"  Max:  {max(times)*1000:.1f}ms")
        print(f"  Records: {len(result['data'])}")
        print()

    # Benchmark get_training_data with profile filter
    times = []
    for i in range(iterations):
        start = time.time()
        result = await service.get_training_data(profile="collector", limit=1000)
        elapsed = time.time() - start
        times.append(elapsed)
    print(f"get_training_data(profile='collector', limit=1000):")
    print(f"  Mean: {statistics.mean(times)*1000:.1f}ms")
    print(f"  Min:  {min(times)*1000:.1f}ms")
    print(f"  Max:  {max(times)*1000:.1f}ms")
    print(f"  Records: {len(result['data'])}")
    print()

    # Benchmark get_profile_coverage
    times = []
    for i in range(iterations):
        start = time.time()
        result = await service.get_profile_coverage()
        elapsed = time.time() - start
        times.append(elapsed)
    print(f"get_profile_coverage():")
    print(f"  Mean: {statistics.mean(times)*1000:.1f}ms")
    print(f"  Min:  {min(times)*1000:.1f}ms")
    print(f"  Max:  {max(times)*1000:.1f}ms")
    for p in result['profiles']:
        print(f"  {p['name']}: {p['coverage_percent']:.1f}% ({p['total_available']}/{p['total_expected']})")
    print()

    # Benchmark get_data_quality with various lookback windows
    for hours in [1, 24, 168]:
        times = []
        for i in range(iterations):
            start = time.time()
            result = await service.get_data_quality(hours=hours)
            elapsed = time.time() - start
            times.append(elapsed)
        print(f"get_data_quality(hours={hours}):")
        print(f"  Mean: {statistics.mean(times)*1000:.1f}ms")
        print(f"  Min:  {min(times)*1000:.1f}ms")
        print(f"  Max:  {max(times)*1000:.1f}ms")
        print(f"  Score: {result['summary']['overall_score']:.1f}")
        print()

    print("=" * 60)
    print("Benchmark complete")
    print("=" * 60)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(benchmark())
