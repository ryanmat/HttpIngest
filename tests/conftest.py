# Description: Pytest fixtures for HttpIngest tests
# Description: Provides sample OTLP payloads used across the test suite

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(scope="session")
def sample_otlp_data() -> dict[str, Any]:
    """Load sample OTLP JSON data from tests/fixtures/sample_otlp.json."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_otlp.json"
    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def sample_otlp_cpu_metrics() -> dict[str, Any]:
    """Sample OTLP data with CPU metrics from a generic OTLP exporter."""
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "web-server"}},
                        {"key": "host.name", "value": {"stringValue": "server01"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "CPU_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "cpu.usage",
                                "unit": "percent",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asDouble": 45.2,
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


@pytest.fixture(scope="session")
def sample_otlp_memory_metrics() -> dict[str, Any]:
    """Sample OTLP data with memory metrics from a generic OTLP exporter."""
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "database"}},
                        {"key": "host.name", "value": {"stringValue": "db-server-01"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "Memory_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "memory.usage",
                                "unit": "bytes",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asInt": 8589934592,
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


@pytest.fixture(scope="session")
def sample_otlp_multi_metric() -> dict[str, Any]:
    """Sample OTLP data with multiple metrics across multiple resources."""
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "web-server"}},
                        {"key": "host.name", "value": {"stringValue": "server01"}},
                        {"key": "environment", "value": {"stringValue": "production"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "CPU_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "cpu.usage",
                                "unit": "percent",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asDouble": 45.2,
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                    {
                        "scope": {"name": "Memory_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "memory.usage",
                                "unit": "bytes",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asInt": 4294967296,
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "database"}},
                        {"key": "host.name", "value": {"stringValue": "db-server-01"}},
                        {"key": "environment", "value": {"stringValue": "production"}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "Disk_Usage", "version": "1.0"},
                        "metrics": [
                            {
                                "name": "disk.usage",
                                "unit": "percent",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": 1699123456000000000,
                                            "asDouble": 78.5,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            },
        ]
    }
