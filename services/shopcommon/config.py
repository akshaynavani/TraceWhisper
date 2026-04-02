"""Environment-driven settings shared by every service."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    service_name: str = field(default_factory=lambda: _env("SERVICE_NAME", "unknown-service"))
    service_version: str = field(default_factory=lambda: _env("SERVICE_VERSION", "0.1.0"))
    environment: str = field(default_factory=lambda: _env("DEPLOY_ENV", "local"))

    db_path: str = field(default_factory=lambda: _env("DB_PATH", "/data/service.db"))

    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:9092"))
    orders_topic: str = field(default_factory=lambda: _env("ORDERS_TOPIC", "orders.placed"))
    orders_dlq_topic: str = field(default_factory=lambda: _env("ORDERS_DLQ_TOPIC", "orders.placed.dlq"))

    catalog_url: str = field(default_factory=lambda: _env("CATALOG_URL", "http://catalog:8000"))
    orders_url: str = field(default_factory=lambda: _env("ORDERS_URL", "http://orders:8000"))
    http_timeout_s: float = field(default_factory=lambda: float(_env("HTTP_TIMEOUT_S", "3.0")))

    otlp_endpoint: str = field(
        default_factory=lambda: _env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    )
    tracing_enabled: bool = field(
        default_factory=lambda: _env("TRACING_ENABLED", "true").lower() in ("1", "true", "yes")
    )


def get_settings() -> Settings:
    return Settings()
