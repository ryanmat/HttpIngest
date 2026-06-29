# Description: OpenTelemetry tracing configuration for HttpIngest.
# Description: Generic OTLP/HTTP exporter wired to env-driven endpoint and headers.

"""
OpenTelemetry tracing setup for HttpIngest container app.

Supports two exporter types:
- otlp: Generic OTLP/HTTP exporter (configured via OTEL_EXPORTER_OTLP_ENDPOINT
  and optional OTEL_EXPORTER_OTLP_HEADERS).
- console: Span printing for local debugging.

Environment variables:
- OTEL_TRACING_ENABLED: Master toggle (default: true).
- OTEL_SERVICE_NAME: Service name (default: httpingest).
- OTEL_SERVICE_VERSION: Service version (defaults to package version).
- OTEL_SERVICE_NAMESPACE: Namespace label for grouping services.
- OTEL_EXPORTER_TYPE: One of "otlp" or "console" (default: otlp).
- OTEL_EXPORTER_OTLP_ENDPOINT: Collector URL, e.g. http://otel-collector:4318/v1/traces.
- OTEL_EXPORTER_OTLP_HEADERS: Comma-separated key=value pairs (e.g. "Authorization=Bearer xyz").
- OTEL_TRACES_SAMPLER_ARG: Sampling rate 0.0-1.0 (default: 1.0).
- OTEL_DEBUG: Verbose otel logger (default: false).
- OTEL_VERBOSE: Wrap exporter with logging exporter (default: true).
- OTEL_DEBUG_CONSOLE: Add console processor in addition to OTLP (default: false).
"""

import logging
import os
from collections.abc import Sequence

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

logger = logging.getLogger(__name__)

# Enable verbose OTEL logging when debug is enabled
if os.getenv("OTEL_DEBUG", "false").lower() == "true":
    logging.getLogger("opentelemetry").setLevel(logging.DEBUG)


class LoggingSpanExporter(SpanExporter):
    """Wrapper exporter that logs export results for debugging."""

    def __init__(self, wrapped_exporter: SpanExporter, exporter_name: str = "unknown"):
        self._wrapped = wrapped_exporter
        self._name = exporter_name
        self._export_count = 0
        self._success_count = 0
        self._failure_count = 0

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self._export_count += 1
        span_count = len(spans)

        try:
            result = self._wrapped.export(spans)

            if result == SpanExportResult.SUCCESS:
                self._success_count += 1
                logger.info(
                    f"[{self._name}] Exported {span_count} spans successfully "
                    f"(total: {self._success_count}/{self._export_count})"
                )
            else:
                self._failure_count += 1
                logger.error(
                    f"[{self._name}] Failed to export {span_count} spans: {result} "
                    f"(failures: {self._failure_count}/{self._export_count})"
                )

            return result

        except Exception as e:
            self._failure_count += 1
            logger.error(
                f"[{self._name}] Exception exporting {span_count} spans: {e} "
                f"(failures: {self._failure_count}/{self._export_count})"
            )
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        logger.info(
            f"[{self._name}] Shutting down. Stats: "
            f"exports={self._export_count}, success={self._success_count}, "
            f"failures={self._failure_count}"
        )
        self._wrapped.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._wrapped.force_flush(timeout_millis)


# Version should match pyproject.toml
SERVICE_VERSION_VALUE = "1.0.0"


def _parse_headers(raw: str) -> dict:
    """Parse comma-separated key=value pairs into a dict."""
    headers: dict = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        key, _, value = pair.partition("=")
        key = key.strip()
        value = value.strip()
        if key:
            headers[key] = value
    return headers


def get_tracing_config() -> dict:
    """Get tracing configuration from environment."""
    return {
        "service_name": os.getenv("OTEL_SERVICE_NAME", "httpingest"),
        "service_version": os.getenv("OTEL_SERVICE_VERSION", SERVICE_VERSION_VALUE),
        "exporter_type": os.getenv("OTEL_EXPORTER_TYPE", "otlp"),
        "otlp_endpoint": os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
        "otlp_headers": os.getenv("OTEL_EXPORTER_OTLP_HEADERS", ""),
        "sample_rate": float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1.0")),
        "enabled": os.getenv("OTEL_TRACING_ENABLED", "true").lower() == "true",
    }


def create_otlp_exporter(config: dict) -> OTLPSpanExporter | None:
    """Create generic OTLP/HTTP exporter from config."""
    endpoint = config["otlp_endpoint"]

    if not endpoint:
        logger.warning("OTEL_EXPORTER_OTLP_ENDPOINT not set, cannot create OTLP exporter")
        return None

    headers = _parse_headers(config["otlp_headers"]) if config["otlp_headers"] else None

    if headers:
        return OTLPSpanExporter(endpoint=endpoint, headers=headers)
    return OTLPSpanExporter(endpoint=endpoint)


def setup_tracing(app=None) -> TracerProvider | None:
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

    resource = Resource.create(
        {
            SERVICE_NAME: config["service_name"],
            SERVICE_VERSION: config["service_version"],
            "service.namespace": os.getenv("OTEL_SERVICE_NAMESPACE", "httpingest"),
            "deployment.environment": os.getenv("ENVIRONMENT", "development"),
        }
    )

    sampler = TraceIdRatioBased(config["sample_rate"])
    provider = TracerProvider(resource=resource, sampler=sampler)

    exporter: SpanExporter | None = None
    exporter_type = config["exporter_type"].lower()

    if exporter_type == "otlp":
        exporter = create_otlp_exporter(config)
    elif exporter_type == "console":
        exporter = ConsoleSpanExporter()
    else:
        logger.warning(f"Unknown exporter type: {exporter_type}, falling back to console")
        exporter = ConsoleSpanExporter()

    if exporter:
        verbose = os.getenv("OTEL_VERBOSE", "true").lower() == "true"
        if verbose and not isinstance(exporter, ConsoleSpanExporter):
            exporter = LoggingSpanExporter(exporter, exporter_type)
            logger.info(f"Enabled verbose export logging for {exporter_type}")

        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        logger.info(f"Added {exporter_type} span processor")

    if os.getenv("OTEL_DEBUG_CONSOLE", "false").lower() == "true":
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        console_processor = SimpleSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(console_processor)
        logger.info("Added debug console span processor")

    trace.set_tracer_provider(provider)
    _instrument_libraries(app)

    logger.info("Tracing setup complete")
    return provider


def _instrument_libraries(app=None):
    """Auto-instrument common libraries."""
    if app:
        try:
            FastAPIInstrumentor.instrument_app(
                app,
                excluded_urls="health,api/health,metrics,api/HttpIngest",
                http_capture_headers_server_request=[],
                http_capture_headers_server_response=[],
            )
            logger.info("Instrumented FastAPI app")
        except Exception as e:
            logger.warning(f"FastAPI instrumentation failed: {e}")

    try:
        HTTPXClientInstrumentor().instrument()
        logger.info("Instrumented httpx")
    except Exception as e:
        logger.warning(f"Failed to instrument httpx: {e}")

    try:
        LoggingInstrumentor().instrument(set_logging_format=True)
        logger.info("Instrumented logging")
    except Exception as e:
        logger.warning(f"Failed to instrument logging: {e}")


def get_tracer(name: str = __name__) -> trace.Tracer:
    """Get a tracer instance for manual instrumentation."""
    return trace.get_tracer(name)


def add_span_attributes(attributes: dict):
    """Add attributes to the current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, value)


def record_exception(exception: Exception, attributes: dict | None = None):
    """Record an exception on the current span."""
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
