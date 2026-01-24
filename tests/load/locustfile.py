# Description: Load testing configuration for LogicMonitor Data Pipeline using Locust
# Description: Tests OTLP ingestion, exports, and real-time streaming under load

import json
import gzip
import io
import time
from datetime import datetime
from locust import HttpUser, task, between, events
from locust.contrib.fasthttp import FastHttpUser


class OTLPDataGenerator:
    """Generate realistic OTLP payloads for testing."""

    @staticmethod
    def generate_otlp_payload(num_metrics=10, num_resources=1):
        """
        Generate OTLP payload with specified metrics and resources.

        Args:
            num_metrics: Number of metrics to include
            num_resources: Number of resources to include

        Returns:
            dict: OTLP formatted payload
        """
        timestamp_ns = int(time.time() * 1e9)

        resource_metrics = []
        for r in range(num_resources):
            metrics = []
            for m in range(num_metrics):
                metric_name = f"test.metric.{m}"

                # Alternate between gauge and sum metrics
                if m % 2 == 0:
                    metric = {
                        "name": metric_name,
                        "description": f"Test metric {m}",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "asDouble": 42.5 + m,
                                "timeUnixNano": timestamp_ns,
                                "attributes": []
                            }]
                        }
                    }
                else:
                    metric = {
                        "name": metric_name,
                        "description": f"Test metric {m}",
                        "unit": "bytes",
                        "sum": {
                            "aggregationTemporality": 2,  # CUMULATIVE
                            "isMonotonic": True,
                            "dataPoints": [{
                                "asInt": str(1000 + m),
                                "timeUnixNano": timestamp_ns,
                                "attributes": []
                            }]
                        }
                    }

                metrics.append(metric)

            resource_metrics.append({
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": f"load-test-service-{r}"}},
                        {"key": "host.name", "value": {"stringValue": f"load-test-host-{r}"}},
                        {"key": "service.instance.id", "value": {"stringValue": f"instance-{r}"}}
                    ]
                },
                "scopeMetrics": [{
                    "scope": {"name": "load-test-scope", "version": "1.0.0"},
                    "metrics": metrics
                }]
            })

        return {"resourceMetrics": resource_metrics}

    @staticmethod
    def compress_payload(payload):
        """Compress payload with gzip."""
        json_bytes = json.dumps(payload).encode('utf-8')
        compressed = io.BytesIO()

        with gzip.GzipFile(fileobj=compressed, mode='wb') as gz:
            gz.write(json_bytes)

        return compressed.getvalue()


class DataPipelineUser(FastHttpUser):
    """
    Load test user for LogicMonitor Data Pipeline.

    Simulates realistic user behavior:
    - Ingesting OTLP data
    - Exporting metrics via Prometheus
    - Querying via Grafana
    - Health checks
    """

    wait_time = between(1, 3)  # Wait 1-3 seconds between tasks
    host = "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io"

    def on_start(self):
        """Initialize user session."""
        self.generator = OTLPDataGenerator()
        self.client_id = f"load-test-{self.user_id}"

    @task(10)
    def ingest_otlp_data(self):
        """
        Ingest OTLP data (most common operation).

        Weight: 10 (runs 10x more than other tasks)
        """
        payload = self.generator.generate_otlp_payload(num_metrics=50, num_resources=1)

        # Test uncompressed
        self.client.post(
            "/api/HttpIngest",
            json=payload,
            headers={"Content-Type": "application/json"},
            name="/api/HttpIngest (uncompressed)"
        )

    @task(5)
    def ingest_otlp_compressed(self):
        """
        Ingest compressed OTLP data.

        Weight: 5
        """
        payload = self.generator.generate_otlp_payload(num_metrics=100, num_resources=2)
        compressed = self.generator.compress_payload(payload)

        self.client.post(
            "/api/HttpIngest",
            data=compressed,
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip"
            },
            name="/api/HttpIngest (gzip)"
        )

    @task(3)
    def export_prometheus(self):
        """
        Export metrics in Prometheus format.

        Weight: 3
        """
        self.client.get(
            "/metrics/prometheus?hours=1",
            name="/metrics/prometheus"
        )

    @task(2)
    def grafana_search(self):
        """
        Search metrics via Grafana datasource.

        Weight: 2
        """
        self.client.post(
            "/grafana/search",
            json={"target": "test"},
            name="/grafana/search"
        )

    @task(2)
    def grafana_query(self):
        """
        Query time-series data via Grafana.

        Weight: 2
        """
        now = datetime.now()
        one_hour_ago = datetime.fromtimestamp(now.timestamp() - 3600)

        self.client.post(
            "/grafana/query",
            json={
                "targets": [{"target": "test.metric.0"}],
                "range": {
                    "from": one_hour_ago.isoformat(),
                    "to": now.isoformat()
                },
                "maxDataPoints": 100
            },
            name="/grafana/query"
        )

    @task(1)
    def export_csv(self):
        """
        Export metrics as CSV.

        Weight: 1 (least common)
        """
        self.client.get(
            "/export/csv?metrics=test.metric.0&hours=1",
            name="/export/csv"
        )

    @task(5)
    def health_check(self):
        """
        Check application health.

        Weight: 5
        """
        self.client.get("/api/health", name="/api/health")

    @task(1)
    def metrics_summary(self):
        """
        Get metrics summary.

        Weight: 1
        """
        self.client.get("/api/metrics/summary", name="/api/metrics/summary")


class SpikeTestUser(DataPipelineUser):
    """
    User for spike testing - bursts of high traffic.

    No wait time between requests.
    """
    wait_time = between(0, 0.1)  # Minimal wait for spike testing


# Event handlers for custom metrics

@events.init_command_line_parser.add_listener
def _(parser):
    """Add custom command line arguments."""
    parser.add_argument("--test-duration", type=int, default=60,
                        help="Duration of load test in seconds")
    parser.add_argument("--target-rps", type=int, default=100,
                        help="Target requests per second")


@events.test_start.add_listener
def _(environment, **kwargs):
    """Log test start."""
    print(f"Starting load test")
    print(f"   Target: {environment.host}")
    print(f"   Users: {environment.runner.target_user_count}")


@events.test_stop.add_listener
def _(environment, **kwargs):
    """Log test results summary."""
    stats = environment.stats
    print(f"\nLoad Test Summary")
    print(f"   Total Requests: {stats.total.num_requests}")
    print(f"   Failures: {stats.total.num_failures}")
    print(f"   Median Response Time: {stats.total.median_response_time}ms")
    print(f"   95th Percentile: {stats.total.get_response_time_percentile(0.95)}ms")
    print(f"   Requests/sec: {stats.total.total_rps}")
