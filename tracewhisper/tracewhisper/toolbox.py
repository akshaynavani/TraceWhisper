"""Investigation tools shared by the MCP server and the CLI.

Kept free of MCP/LLM concerns so it can be unit-tested with mocked HTTP.
Every method returns JSON-serialisable data sized for an LLM context window.
"""

from __future__ import annotations

from typing import Any

from .config import Config
from .correlate import analyze, build_baseline
from .jaeger import JaegerClient
from .models import Trace
from .prometheus import PrometheusClient
from .report import findings_to_dict, summarize_traces


class Toolbox:
    def __init__(self, config: Config, jaeger: JaegerClient | None = None, prom: PrometheusClient | None = None):
        self.config = config
        self.jaeger = jaeger or JaegerClient(config.jaeger_url, config.http_timeout_s)
        self.prom = prom or PrometheusClient(config.prometheus_url, config.http_timeout_s)

    async def aclose(self) -> None:
        await self.jaeger.aclose()
        await self.prom.aclose()

    # ------------------------------------------------------------------
    async def list_services(self) -> dict[str, Any]:
        services = await self.jaeger.services()
        return {svc: await self.jaeger.operations(svc) for svc in services}

    async def find_traces(
        self,
        service: str,
        operation: str | None = None,
        lookback: str = "15m",
        errors_only: bool = False,
        min_duration_ms: int | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        traces = await self.jaeger.find_traces(
            service,
            operation=operation,
            lookback=lookback,
            limit=max(1, min(limit, 100)),
            errors_only=errors_only,
            min_duration=f"{min_duration_ms}ms" if min_duration_ms else None,
        )
        summary = summarize_traces(traces)
        summary["query"] = {
            "service": service,
            "operation": operation,
            "lookback": lookback,
            "errors_only": errors_only,
            "min_duration_ms": min_duration_ms,
        }
        return summary

    async def _baseline_for(self, trace: Trace, lookback: str, sample: int = 50):
        root = trace.root
        candidates = await self.jaeger.find_traces(
            root.service, operation=root.operation, lookback=lookback, limit=sample
        )
        healthy = [t for t in candidates if t.trace_id != trace.trace_id and not t.has_error]
        return build_baseline(healthy), len(healthy)

    async def analyze_trace(self, trace_id: str, baseline_lookback: str = "1h") -> dict[str, Any]:
        trace = await self.jaeger.get_trace(trace_id)
        baseline, n = await self._baseline_for(trace, baseline_lookback)
        findings = analyze(trace, baseline, slow_factor=self.config.slow_factor)
        data = findings_to_dict(findings)
        data["baseline"] = {"healthy_traces": n, "lookback": baseline_lookback}
        return data

    async def span_details(self, trace_id: str, span_id: str) -> dict[str, Any]:
        trace = await self.jaeger.get_trace(trace_id)
        span = trace.by_id.get(span_id)
        if span is None:
            raise ValueError(f"span {span_id} not in trace {trace_id}")
        parent = trace.parent(span)
        return {
            "span": span.summary(),
            "all_tags": span.tags,
            "events": [{"name": e.name, "attributes": e.attributes} for e in span.events],
            "parent": parent.summary() if parent else None,
            "children": [c.summary() for c in span.children],
        }

    async def service_metrics(self, service: str | None = None, window: str = "5m") -> dict[str, Any]:
        return await self.prom.service_health(window=window, service=service)

    async def promql(self, expr: str) -> list[dict[str, Any]]:
        samples = await self.prom.query(expr)
        return [{"labels": s.labels, "value": s.value} for s in samples[:50]]

    async def service_dependencies(self, lookback_minutes: int = 60) -> list[dict[str, Any]]:
        deps = await self.jaeger.dependencies(lookback_ms=lookback_minutes * 60_000)
        return [
            {"caller": d.get("parent"), "callee": d.get("child"), "calls": d.get("callCount")} for d in deps
        ]
