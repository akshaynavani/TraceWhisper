"""Thin async client for the Jaeger Query HTTP API (/api/...)."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from .models import Trace


class JaegerError(RuntimeError):
    pass


class JaegerClient:
    def __init__(self, base_url: str, timeout_s: float = 10.0, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout_s)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "JaegerClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        try:
            resp = await self._client.get(f"{self.base_url}{path}", params=params)
        except httpx.HTTPError as exc:
            raise JaegerError(f"cannot reach Jaeger at {self.base_url}: {exc}") from exc
        if resp.status_code == 404:
            raise JaegerError(f"not found: {path}")
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise JaegerError(str(body["errors"]))
        return body.get("data")

    async def services(self) -> list[str]:
        data = await self._get("/api/services") or []
        return sorted(s for s in data if s != "jaeger-all-in-one")

    async def operations(self, service: str) -> list[str]:
        return sorted(await self._get(f"/api/services/{service}/operations") or [])

    async def find_traces(
        self,
        service: str,
        *,
        operation: str | None = None,
        lookback: str = "1h",
        limit: int = 20,
        tags: dict[str, str] | None = None,
        min_duration: str | None = None,
        max_duration: str | None = None,
        errors_only: bool = False,
    ) -> list[Trace]:
        params: dict[str, Any] = {"service": service, "lookback": lookback, "limit": limit}
        if operation:
            params["operation"] = operation
        merged_tags = dict(tags or {})
        if errors_only:
            merged_tags["error"] = "true"
        if merged_tags:
            params["tags"] = json.dumps(merged_tags)
        if min_duration:
            params["minDuration"] = min_duration
        if max_duration:
            params["maxDuration"] = max_duration
        data = await self._get("/api/traces", params) or []
        return [Trace.from_jaeger(t) for t in data if t.get("spans")]

    async def get_trace(self, trace_id: str) -> Trace:
        data = await self._get(f"/api/traces/{trace_id}") or []
        if not data:
            raise JaegerError(f"trace {trace_id} not found")
        return Trace.from_jaeger(data[0])

    async def dependencies(self, lookback_ms: int = 3_600_000) -> list[dict[str, Any]]:
        end_ts = int(time.time() * 1000)
        return await self._get("/api/dependencies", {"endTs": end_ts, "lookback": lookback_ms}) or []
