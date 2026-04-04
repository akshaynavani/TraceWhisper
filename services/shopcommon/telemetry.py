"""OpenTelemetry bootstrap shared by all services.

Traces are exported over OTLP/gRPC to the OpenTelemetry Collector, which
forwards them to Jaeger and derives RED metrics (spanmetrics) for Prometheus.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode

from .config import Settings

log = logging.getLogger(__name__)

_configured = False
LOG_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s "
    "[trace_id=%(otelTraceID)s span_id=%(otelSpanID)s] %(message)s"
)


def setup_tracing(settings: Settings) -> None:
    """Install a global TracerProvider exactly once per process."""
    global _configured
    if _configured:
        return
    resource = Resource.create(
        {
            "service.name": settings.service_name,
            "service.version": settings.service_version,
            "deployment.environment": settings.environment,
        }
    )
    provider = TracerProvider(resource=resource)
    if settings.tracing_enabled:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True))
        )
    trace.set_tracer_provider(provider)

    HTTPXClientInstrumentor().instrument()
    LoggingInstrumentor().instrument(set_logging_format=False)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    _configured = True
    log.info("tracing configured for %s -> %s", settings.service_name, settings.otlp_endpoint)


def instrument_app(app: FastAPI, settings: Settings) -> None:
    setup_tracing(settings)
    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,metrics")


def get_tracer(name: str = "tracewhisper.shop") -> trace.Tracer:
    return trace.get_tracer(name)


def mark_error(span: Span | None, exc: BaseException, **attributes: Any) -> None:
    """Record an exception on a span and flag it as failed."""
    span = span or trace.get_current_span()
    if not span.is_recording():
        return
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
    for key, value in attributes.items():
        span.set_attribute(key, value)


def current_trace_id() -> str | None:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else None
