"""
ABOUTME: Load test script for async HTTP ingestion endpoint
ABOUTME: Sends concurrent OTLP requests and measures response times
"""

import asyncio
import aiohttp
import time
import json
from datetime import datetime
import statistics


async def send_request(session, url, payload, request_id):
    """Send a single OTLP ingestion request and measure time."""
    start = time.time()
    try:
        async with session.post(
            url,
            json=payload,
            headers={"content-type": "application/json"}
        ) as response:
            end = time.time()
            response_time = end - start
            status = response.status

            if status == 200:
                data = await response.json()
                return {
                    "id": request_id,
                    "status": status,
                    "response_time": response_time,
                    "success": True,
                    "metric_id": data.get("id")
                }
            else:
                text = await response.text()
                return {
                    "id": request_id,
                    "status": status,
                    "response_time": response_time,
                    "success": False,
                    "error": text
                }
    except Exception as e:
        end = time.time()
        return {
            "id": request_id,
            "status": 0,
            "response_time": end - start,
            "success": False,
            "error": str(e)
        }


async def run_load_test(url, num_requests=50, concurrency=10):
    """
    Run load test with specified number of concurrent requests.

    Args:
        url: The ingestion endpoint URL
        num_requests: Total number of requests to send
        concurrency: Number of concurrent requests
    """
    # Create a valid OTLP payload
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "load-test"}},
                        {"key": "host.name", "value": {"stringValue": "test-host"}}
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "load-test"},
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

    print(f"\n🔥 Starting load test...")
    print(f"   URL: {url}")
    print(f"   Total requests: {num_requests}")
    print(f"   Concurrency: {concurrency}")
    print(f"   Expected duration: ~{num_requests / concurrency} seconds\n")

    start_time = time.time()

    # Create session with connection pooling
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Create batches of concurrent requests
        results = []

        for batch_start in range(0, num_requests, concurrency):
            batch_end = min(batch_start + concurrency, num_requests)
            batch_size = batch_end - batch_start

            print(f"Sending batch {batch_start // concurrency + 1} ({batch_size} requests)...", end=" ")

            tasks = [
                send_request(session, url, payload, i)
                for i in range(batch_start, batch_end)
            ]

            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

            # Show progress
            successful = sum(1 for r in batch_results if r["success"])
            avg_time = statistics.mean(r["response_time"] for r in batch_results)
            print(f"✓ {successful}/{batch_size} successful, avg={avg_time:.3f}s")

    end_time = time.time()
    total_time = end_time - start_time

    # Analyze results
    print(f"\n{'='*60}")
    print(f"📊 LOAD TEST RESULTS")
    print(f"{'='*60}\n")

    successful_results = [r for r in results if r["success"]]
    failed_results = [r for r in results if not r["success"]]

    print(f"Total requests:     {num_requests}")
    print(f"Successful:         {len(successful_results)} ({len(successful_results)/num_requests*100:.1f}%)")
    print(f"Failed:             {len(failed_results)} ({len(failed_results)/num_requests*100:.1f}%)")
    print(f"Total time:         {total_time:.2f}s")
    print(f"Requests/sec:       {num_requests/total_time:.2f}\n")

    if successful_results:
        response_times = [r["response_time"] for r in successful_results]

        print(f"Response Times (successful requests):")
        print(f"  Min:              {min(response_times):.3f}s")
        print(f"  Max:              {max(response_times):.3f}s")
        print(f"  Average:          {statistics.mean(response_times):.3f}s")
        print(f"  Median:           {statistics.median(response_times):.3f}s")
        print(f"  Std Dev:          {statistics.stdev(response_times) if len(response_times) > 1 else 0:.3f}s")

        # Percentiles
        sorted_times = sorted(response_times)
        p50 = sorted_times[int(len(sorted_times) * 0.50)]
        p95 = sorted_times[int(len(sorted_times) * 0.95)]
        p99 = sorted_times[int(len(sorted_times) * 0.99)]

        print(f"\nPercentiles:")
        print(f"  P50 (median):     {p50:.3f}s")
        print(f"  P95:              {p95:.3f}s")
        print(f"  P99:              {p99:.3f}s")

        # Performance assessment
        print(f"\n{'='*60}")
        if statistics.mean(response_times) < 0.5:
            print("✅ EXCELLENT - Average response time < 500ms")
        elif statistics.mean(response_times) < 1.0:
            print("✅ GOOD - Average response time < 1s")
        elif statistics.mean(response_times) < 2.0:
            print("⚠️  ACCEPTABLE - Average response time < 2s")
        else:
            print("❌ POOR - Average response time > 2s")

        if p95 < 1.0:
            print("✅ EXCELLENT - 95th percentile < 1s")
        elif p95 < 2.0:
            print("✅ GOOD - 95th percentile < 2s")
        else:
            print("⚠️  NEEDS IMPROVEMENT - 95th percentile > 2s")

        print(f"{'='*60}\n")

    if failed_results:
        print(f"\n❌ Failed Requests Details:")
        for result in failed_results[:5]:  # Show first 5 failures
            print(f"  Request {result['id']}: {result.get('error', 'Unknown error')}")
        if len(failed_results) > 5:
            print(f"  ... and {len(failed_results) - 5} more")

    return results


if __name__ == "__main__":
    import sys

    # Default to production endpoint
    url = "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/HttpIngest"

    # Parse command line args
    num_requests = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    # Run the load test
    asyncio.run(run_load_test(url, num_requests, concurrency))
