# Description: OpenTelemetry tracing configuration for HttpIngest.
# Description: Provides distributed tracing with LogicMonitor APM integration.

"""
OpenTelemetry tracing setup for HttpIngest container app.

Supports multiple exporters:
- LogicMonitor APM (OTLP/gRPC or OTLP/HTTP)
- Console (for local debugging)
- OTLP endpoint (generic)

Environment variables:
- OTEL_SERVICE_NAME: Service name (default: httpingest)
- OTEL_EXPORTER_TYPE: Exporter type (logicmonitor, otlp, console)
- LM_ACCOUNT: LogicMonitor account name
- LM_OTEL_TOKEN: LogicMonitor bearer token for OTLP
- OTEL_EXPORTER_OTLP_ENDPOINT: Generic OTLP endpoint
- OTEL_TRACES_SAMPLER_ARG: Sampling rate (0.0-1.0, default: 1.0)
"""

import logging
import os
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

logger = logging.getLogger(__name__)

# Version should match pyproject.toml
SERVICE_VERSION_VALUE = "39.0.0"


def get_tracing_config() -> dict:
    """Get tracing configuration from environment."""
    return {
        "service_name": os.getenv("OTEL_SERVICE_NAME", "httpingest"),
        "service_version": os.getenv("OTEL_SERVICE_VERSION", SERVICE_VERSION_VALUE),
        "exporter_type": os.getenv("OTEL_EXPORTER_TYPE", "console"),
        "lm_account": os.getenv("LM_ACCOUNT", ""),
        "lm_otel_token": os.getenv("LM_OTEL_TOKEN", ""),
        "otlp_endpoint": os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
        "sample_rate": float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1.0")),
        "enabled": os.getenv("OTEL_TRACING_ENABLED", "true").lower() == "true",
    }


def create_lm_exporter(config: dict) -> Optional[OTLPSpanExporter]:
    """Create LogicMonitor OTLP exporter."""
    account = config["lm_account"]
    token = config["lm_otel_token"]

    if not account or not token:
        logger.warning("LM_ACCOUNT or LM_OTEL_TOKEN not set, cannot create LM exporter")
        return None

    endpoint = f"https://{account}.logicmonitor.com/rest/api/v1/traces"

    return OTLPSpanExporter(
        endpoint=endpoint,
        headers={"Authorization": f"Bearer {token}"},
    )


def create_otlp_exporter(config: dict) -> Optional[OTLPSpanExporter]:
    """Create generic OTLP exporter."""
    endpoint = config["otlp_endpoint"]

    if not endpoint:
        logger.warning("OTEL_EXPORTER_OTLP_ENDPOINT not set, cannot create OTLP exporter")
        return None

    return OTLPSpanExporter(endpoint=endpoint)


def setup_tracing(app=None) -> Optional[TracerProvider]:
    """Initialize OpenTelemetry tracing.

    Args:
        app: Optional FastAPI application to instrument.

    Returns:
        TracerProvider if tracing is enabled, None otherwise.
    """
    config = get_tracing_config()

    if not config["enabled"]:
        logger.info("Tracing disabled via OTEL_TRACING_ENABLED=false")
        return None

    logger.info(
        f"Setting up tracing: service={config['service_name']}, "
        f"exporter={config['exporter_type']}, sample_rate={config['sample_rate']}"
    )

    # Create resource with service info
    resource = Resource.create({
        SERVICE_NAME: config["service_name"],
        SERVICE_VERSION: config["service_version"],
        "deployment.environment": os.getenv("ENVIRONMENT", "development"),
    })

    # Create sampler
    sampler = TraceIdRatioBased(config["sample_rate"])

    # Create tracer provider
    provider = TracerProvider(resource=resource, sampler=sampler)

    # Create exporter based on type
    exporter = None
    exporter_type = config["exporter_type"].lower()

    if exporter_type == "logicmonitor":
        exporter = create_lm_exporter(config)
    elif exporter_type == "otlp":
        exporter = create_otlp_exporter(config)
    elif exporter_type == "console":
        exporter = ConsoleSpanExporter()
    else:
        logger.warning(f"Unknown exporter type: {exporter_type}, falling back to console")
        exporter = ConsoleSpanExporter()

    if exporter:
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        logger.info(f"Added {exporter_type} span processor")

    # Set as global tracer provider
    trace.set_tracer_provider(provider)

    # Auto-instrument libraries
    _instrument_libraries(app)

    logger.info("Tracing setup complete")
    return provider


def _instrument_libraries(app=None):
    """Auto-instrument common libraries."""
    # FastAPI instrumentation
    if app:
        FastAPIInstrumentor.instrument_app(
            app,
            excluded_urls="health,metrics",  # Don't trace health checks
        )
        logger.info("Instrumented FastAPI")

    # AsyncPG (PostgreSQL) instrumentation
    try:
        AsyncPGInstrumentor().instrument()
        logger.info("Instrumented asyncpg")
    except Exception as e:
        logger.warning(f"Failed to instrument asyncpg: {e}")

    # HTTPX (HTTP client) instrumentation
    try:
        HTTPXClientInstrumentor().instrument()
        logger.info("Instrumented httpx")
    except Exception as e:
        logger.warning(f"Failed to instrument httpx: {e}")

    # Logging instrumentation (adds trace context to logs)
    try:
        LoggingInstrumentor().instrument(set_logging_format=True)
        logger.info("Instrumented logging")
    except Exception as e:
        logger.warning(f"Failed to instrument logging: {e}")


def get_tracer(name: str = __name__) -> trace.Tracer:
    """Get a tracer instance for manual instrumentation.

    Args:
        name: Tracer name (usually module name).

    Returns:
        Tracer instance.
    """
    return trace.get_tracer(name)


def add_span_attributes(attributes: dict):
    """Add attributes to the current span.

    Args:
        attributes: Dictionary of attribute key-value pairs.
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, value)


def record_exception(exception: Exception, attributes: Optional[dict] = None):
    """Record an exception on the current span.

    Args:
        exception: The exception to record.
        attributes: Optional additional attributes.
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        span.record_exception(exception)
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)


def shutdown_tracing():
    """Shutdown tracing and flush pending spans."""
    provider = trace.get_tracer_provider()
    if hasattr(provider, "shutdown"):
        provider.shutdown()
        logger.info("Tracing shutdown complete")
