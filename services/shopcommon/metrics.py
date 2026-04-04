"""Prometheus metrics exposed on ``/metrics`` by every service."""

from __future__ import annotations

import time

from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response

HTTP_REQUESTS = Counter(
    "shop_http_requests_total",
    "HTTP requests handled",
    ["service", "method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "shop_http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
HTTP_IN_FLIGHT = Gauge("shop_http_in_flight_requests", "In-flight HTTP requests", ["service"])

KAFKA_MESSAGES = Counter(
    "shop_kafka_messages_total",
    "Kafka messages produced/consumed",
    ["service", "topic", "direction", "result"],
)
KAFKA_PROCESS_LATENCY = Histogram(
    "shop_kafka_process_duration_seconds",
    "Time to process one consumed Kafka message",
    ["service", "topic"],
)
DB_LATENCY = Histogram(
    "shop_db_query_duration_seconds",
    "SQLite statement latency",
    ["service", "op"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
BUSINESS_EVENTS = Counter(
    "shop_business_events_total",
    "Domain-level outcomes (checkout completed, payment failed, order dead-lettered, ...)",
    ["service", "event"],
)


def install_metrics(app: FastAPI, service: str) -> None:
    @app.middleware("http")
    async def _record(request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)
        HTTP_IN_FLIGHT.labels(service).inc()
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            HTTP_IN_FLIGHT.labels(service).dec()
            HTTP_LATENCY.labels(service, request.method, route_path).observe(time.perf_counter() - start)
            HTTP_REQUESTS.labels(service, request.method, route_path, str(status)).inc()

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
