"""MCP server exposing TraceWhisper's telemetry tools.

Run over stdio (default) so any MCP client - the bundled agent, Claude Desktop,
Claude Code - can investigate incidents:

    tracewhisper serve-mcp
"""

from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from .config import load_config
from .toolbox import Toolbox

mcp = FastMCP(
    "tracewhisper",
    instructions=(
        "Telemetry tools for the demo e-commerce system (catalog, checkout, orders over HTTP and Kafka). "
        "Start broad (service_metrics, find_traces with errors_only) and drill into specific traces with "
        "analyze_trace. analyze_trace already separates error origins from propagated errors and compares "
        "spans with a healthy baseline."
    ),
)

_toolbox: Toolbox | None = None


def toolbox() -> Toolbox:
    global _toolbox
    if _toolbox is None:
        _toolbox = Toolbox(load_config())
    return _toolbox


def _json(data: Any) -> str:
    return json.dumps(data, indent=1, default=str)


@mcp.tool()
async def list_services() -> str:
    """List traced services and their span operation names (from Jaeger)."""
    return _json(await toolbox().list_services())


@mcp.tool()
async def find_traces(
    service: str,
    operation: str | None = None,
    lookback: str = "15m",
    errors_only: bool = False,
    min_duration_ms: int | None = None,
    limit: int = 20,
) -> str:
    """Search recent traces for a service and summarise recurring failure points.

    Args:
        service: Service name as reported to Jaeger, e.g. "checkout".
        operation: Optional span operation, e.g. "POST /checkout".
        lookback: Jaeger lookback window such as "15m", "1h", "6h".
        errors_only: Only return traces containing a span tagged error=true.
        min_duration_ms: Only traces at least this slow.
        limit: Max traces to fetch (1-100).
    """
    return _json(
        await toolbox().find_traces(service, operation, lookback, errors_only, min_duration_ms, limit)
    )


@mcp.tool()
async def analyze_trace(trace_id: str, baseline_lookback: str = "1h") -> str:
    """Correlate one trace across services: error origins vs propagated errors, critical path with
    self-times, cross-service edges, Kafka queue gaps, and deviations from healthy baseline traces."""
    return _json(await toolbox().analyze_trace(trace_id, baseline_lookback))


@mcp.tool()
async def span_details(trace_id: str, span_id: str) -> str:
    """Full tags, events (exceptions, stack tails) and neighbours of a single span."""
    return _json(await toolbox().span_details(trace_id, span_id))


@mcp.tool()
async def service_metrics(service: str | None = None, window: str = "5m") -> str:
    """Prometheus RED metrics: request rate, 5xx ratio, p95 latency per service and route, SQLite p95,
    Kafka produce/consume rates and business events (payment_failed, order_dead_lettered, ...)."""
    return _json(await toolbox().service_metrics(service, window))


@mcp.tool()
async def promql(expr: str) -> str:
    """Run an arbitrary instant PromQL query (max 50 series returned)."""
    return _json(await toolbox().promql(expr))


@mcp.tool()
async def service_dependencies(lookback_minutes: int = 60) -> str:
    """Service call graph aggregated by Jaeger over the lookback window."""
    return _json(await toolbox().service_dependencies(lookback_minutes))


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
