"""Minimal Prometheus HTTP API client plus RED-metric helpers for the shop."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


class PrometheusError(RuntimeError):
    pass


@dataclass
class Sample:
    labels: dict[str, str]
    value: float


class PrometheusClient:
    def __init__(self, base_url: str, timeout_s: float = 10.0, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout_s)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def query(self, expr: str) -> list[Sample]:
        try:
            resp = await self._client.get(f"{self.base_url}/api/v1/query", params={"query": expr})
        except httpx.HTTPError as exc:
            raise PrometheusError(f"cannot reach Prometheus at {self.base_url}: {exc}") from exc
        body = resp.json()
        if body.get("status") != "success":
            raise PrometheusError(body.get("error", "query failed"))
        result = body["data"]["result"]
        samples = []
        for item in result:
            raw = item.get("value", [None, "nan"])[1]
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = float("nan")
            samples.append(Sample(labels=item.get("metric", {}), value=value))
        return samples

    async def scalar_by(self, expr: str, label: str) -> dict[str, float]:
        return {s.labels.get(label, ""): s.value for s in await self.query(expr)}

    # -- shop-specific views -------------------------------------------
    async def service_health(self, window: str = "5m", service: str | None = None) -> dict[str, Any]:
        """Rate, error ratio and p95 latency per service (and per route)."""
        sel = f'service="{service}"' if service else ""
        sel_err = f'{sel + "," if sel else ""}status=~"5.."'
        rate = await self.scalar_by(f"sum by (service) (rate(shop_http_requests_total{{{sel}}}[{window}]))", "service")
        errors = await self.scalar_by(
            f"sum by (service) (rate(shop_http_requests_total{{{sel_err}}}[{window}]))", "service"
        )
        p95 = await self.scalar_by(
            "histogram_quantile(0.95, sum by (le, service) "
            f"(rate(shop_http_request_duration_seconds_bucket{{{sel}}}[{window}])))",
            "service",
        )
        route_p95 = await self.query(
            "histogram_quantile(0.95, sum by (le, service, route) "
            f"(rate(shop_http_request_duration_seconds_bucket{{{sel}}}[{window}])))"
        )
        db_p95 = await self.query(
            "histogram_quantile(0.95, sum by (le, service, op) "
            f"(rate(shop_db_query_duration_seconds_bucket{{{sel}}}[{window}])))"
        )
        kafka = await self.query(
            f"sum by (service, topic, direction, result) (rate(shop_kafka_messages_total[{window}]))"
        )
        business = await self.query(f"sum by (service, event) (increase(shop_business_events_total[{window}]))")

        services = sorted(set(rate) | set(errors) | set(p95))
        return {
            "window": window,
            "services": {
                svc: {
                    "req_per_s": _r(rate.get(svc, 0.0)),
                    "error_ratio": _r(errors.get(svc, 0.0) / rate[svc]) if rate.get(svc) else 0.0,
                    "p95_s": _r(p95.get(svc)),
                }
                for svc in services
            },
            "route_p95_s": [
                {"service": s.labels.get("service"), "route": s.labels.get("route"), "p95_s": _r(s.value)}
                for s in route_p95
                if s.value == s.value  # drop NaN
            ],
            "db_p95_s": [
                {"service": s.labels.get("service"), "op": s.labels.get("op"), "p95_s": _r(s.value)}
                for s in db_p95
                if s.value == s.value
            ],
            "kafka_msgs_per_s": [{**s.labels, "rate": _r(s.value)} for s in kafka],
            "business_events": [{**s.labels, "count": _r(s.value)} for s in business],
        }


def _r(value: float | None, digits: int = 4) -> float | None:
    if value is None or value != value:
        return None
    return round(value, digits)
