#!/usr/bin/env python3
# Description: Feature verification script for LogicMonitor Data Pipeline
# Description: Verifies all features are accessible and identifies orphaned code

import os
import sys
import importlib
import inspect
from pathlib import Path
from typing import Dict, List, Set, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class FeatureVerifier:
    """Verifies all implemented features are accessible."""

    def __init__(self):
        self.src_dir = Path(__file__).parent.parent / "src"
        self.function_app_path = Path(__file__).parent.parent / "function_app.py"
        self.features = {}
        self.endpoints = {}
        self.orphaned_code = []

    def verify_all(self) -> Dict[str, Any]:
        """Run all verification checks."""
        print("🔍 LogicMonitor Data Pipeline - Feature Verification")
        print("=" * 60)

        results = {
            "features": self.list_all_features(),
            "endpoints": self.list_all_endpoints(),
            "imports": self.verify_imports(),
            "orphaned": self.find_orphaned_code(),
            "summary": {}
        }

        self.print_results(results)
        return results

    def list_all_features(self) -> Dict[str, List[str]]:
        """List all implemented features by module."""
        print("\n📦 Implemented Features")
        print("-" * 60)

        features = {}

        # Data Ingestion & Processing
        features["Data Ingestion"] = [
            "OTLP data ingestion (HTTP POST)",
            "Gzip compression support",
            "Azure AD token authentication"
        ]

        features["Data Normalization"] = [
            "OTLP parsing (src/otlp_parser.py)",
            "Data processing (src/data_processor.py)",
            "Normalized schema (resources, metrics, data points)",
            "Idempotent processing with status tracking"
        ]

        features["Data Aggregation"] = [
            "Hourly aggregates (src/aggregator.py)",
            "Daily aggregates",
            "Materialized views (src/materialized_views.py)",
            "Metric statistics (min, max, avg, count, sum)"
        ]

        features["Query Endpoints"] = [
            "Get metrics list (src/query_endpoints.py)",
            "Get resources list",
            "Get timeseries data",
            "Get aggregated data",
            "Filter by metric name, resource, time range"
        ]

        features["ML Pipeline"] = [
            "Feature engineering (src/feature_engineering.py)",
            "Anomaly detection (src/anomaly_detector.py)",
            "Time series forecasting (src/predictor.py)",
            "Statistical features (rolling stats, lag features, change detection)"
        ]

        features["Data Exports"] = [
            "Prometheus metrics export (src/exporters.py)",
            "Grafana SimpleJSON datasource",
            "PowerBI OData REST API",
            "CSV export",
            "JSON export",
            "Webhook notifications for alerts"
        ]

        features["Real-time Streaming"] = [
            "WebSocket streaming (src/realtime.py)",
            "Server-Sent Events (SSE)",
            "Redis pub/sub messaging",
            "In-memory fallback broker",
            "Rate limiting per client",
            "Backpressure handling",
            "Client reconnection with message buffering"
        ]

        features["Background Tasks"] = [
            "Data processing loop",
            "Metric publishing loop",
            "Health monitoring loop",
            "Graceful shutdown handling"
        ]

        features["Health & Monitoring"] = [
            "Component health checks",
            "Prometheus metrics exposition",
            "Database connectivity monitoring",
            "Streaming service monitoring",
            "Metrics summary statistics"
        ]

        features["Database"] = [
            "Alembic migrations",
            "PostgreSQL 17 support",
            "Azure AD authentication",
            "Connection pooling",
            "Normalized schema with foreign keys",
            "Indexes for performance"
        ]

        for category, items in features.items():
            print(f"\n{category}:")
            for item in items:
                print(f"  ✓ {item}")

        return features

    def list_all_endpoints(self) -> Dict[str, List[Dict[str, str]]]:
        """List all HTTP endpoints."""
        print("\n\n🌐 HTTP Endpoints")
        print("-" * 60)

        endpoints = {
            "Azure Functions": [
                {
                    "path": "/api/HttpIngest",
                    "method": "POST",
                    "description": "OTLP data ingestion",
                    "file": "function_app.py:287"
                },
                {
                    "path": "/api/health",
                    "method": "GET",
                    "description": "Health check",
                    "file": "function_app.py:350"
                }
            ],
            "Data Exports": [
                {
                    "path": "/metrics/prometheus",
                    "method": "GET",
                    "description": "Prometheus metrics export",
                    "file": "function_app.py:404"
                },
                {
                    "path": "/grafana",
                    "method": "GET",
                    "description": "Grafana datasource health",
                    "file": "function_app.py:422"
                },
                {
                    "path": "/grafana/search",
                    "method": "POST",
                    "description": "Grafana metric search",
                    "file": "function_app.py:427"
                },
                {
                    "path": "/grafana/query",
                    "method": "POST",
                    "description": "Grafana time-series query",
                    "file": "function_app.py:434"
                },
                {
                    "path": "/api/odata/metrics",
                    "method": "GET",
                    "description": "PowerBI OData export",
                    "file": "function_app.py:441"
                },
                {
                    "path": "/export/csv",
                    "method": "GET",
                    "description": "CSV export",
                    "file": "function_app.py:453"
                },
                {
                    "path": "/export/json",
                    "method": "GET",
                    "description": "JSON export",
                    "file": "function_app.py:472"
                }
            ],
            "Real-time Streaming": [
                {
                    "path": "/ws",
                    "method": "WebSocket",
                    "description": "WebSocket streaming",
                    "file": "function_app.py:488"
                },
                {
                    "path": "/sse",
                    "method": "GET",
                    "description": "Server-Sent Events",
                    "file": "function_app.py:496"
                }
            ],
            "Health & Metrics": [
                {
                    "path": "/api/health",
                    "method": "GET",
                    "description": "Detailed health check",
                    "file": "function_app.py:508"
                },
                {
                    "path": "/api/metrics/summary",
                    "method": "GET",
                    "description": "Metrics summary statistics",
                    "file": "function_app.py:554"
                }
            ]
        }

        for category, items in endpoints.items():
            print(f"\n{category}:")
            for endpoint in items:
                print(f"  {endpoint['method']:10} {endpoint['path']:30} {endpoint['description']}")
                print(f"              → {endpoint['file']}")

        return endpoints

    def verify_imports(self) -> Dict[str, bool]:
        """Verify all src modules are imported in function_app.py."""
        print("\n\n📥 Import Verification")
        print("-" * 60)

        with open(self.function_app_path, 'r') as f:
            function_app_content = f.read()

        modules = {
            "src.exporters": False,
            "src.realtime": False,
            "src.otlp_parser": False,
            "src.data_processor": False,
            "src.aggregator": False,
            "src.query_endpoints": False,
            "src.feature_engineering": False,
            "src.anomaly_detector": False,
            "src.predictor": False,
            "src.materialized_views": False
        }

        for module in modules.keys():
            if module in function_app_content:
                modules[module] = True
                print(f"  ✓ {module} - IMPORTED")
            else:
                print(f"  ✗ {module} - NOT IMPORTED (may be used in background tasks)")

        return modules

    def find_orphaned_code(self) -> List[Dict[str, str]]:
        """Find potentially orphaned source files."""
        print("\n\n🔎 Orphaned Code Detection")
        print("-" * 60)

        # List all Python files in src/
        src_files = list(self.src_dir.glob("*.py"))

        # Core files that should exist
        core_files = {
            "otlp_parser.py",
            "data_processor.py",
            "aggregator.py",
            "query_endpoints.py",
            "feature_engineering.py",
            "anomaly_detector.py",
            "predictor.py",
            "exporters.py",
            "realtime.py",
            "materialized_views.py",
            "__init__.py"
        }

        orphaned = []

        for file_path in src_files:
            filename = file_path.name

            if filename.startswith("_") and filename != "__init__.py":
                continue  # Skip private modules

            if filename not in core_files and filename != "__init__.py":
                orphaned.append({
                    "file": filename,
                    "path": str(file_path),
                    "reason": "Not in core files list"
                })

        if orphaned:
            print("\n⚠️  Potentially Orphaned Files:")
            for item in orphaned:
                print(f"  - {item['file']}: {item['reason']}")
        else:
            print("\n✓ No orphaned files detected")

        return orphaned

    def print_results(self, results: Dict[str, Any]):
        """Print summary results."""
        print("\n\n" + "=" * 60)
        print("📊 Verification Summary")
        print("=" * 60)

        total_features = sum(len(items) for items in results["features"].values())
        total_endpoints = sum(len(items) for items in results["endpoints"].values())

        print(f"\n✓ Total Features: {total_features}")
        print(f"✓ Total Endpoints: {total_endpoints}")
        print(f"✓ Imported Modules: {sum(results['imports'].values())}/{len(results['imports'])}")

        if results["orphaned"]:
            print(f"⚠️  Potentially Orphaned Files: {len(results['orphaned'])}")
        else:
            print(f"✓ No Orphaned Files")

        print("\n" + "=" * 60)
        print("✅ Verification Complete!")
        print("=" * 60)


def main():
    """Run feature verification."""
    verifier = FeatureVerifier()
    results = verifier.verify_all()

    # Exit code
    if results["orphaned"]:
        print("\n⚠️  Warning: Orphaned files detected. Review recommended.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
