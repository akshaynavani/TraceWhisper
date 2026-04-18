"""Deterministic trace correlation.

Before any LLM is involved, TraceWhisper reduces a trace (often hundreds of
spans) to a handful of facts that matter for root-cause analysis:

* **error origins** - failing spans none of whose descendants failed. An error
  that merely bubbled up from a child is *propagated*, not an origin.
* **critical path** - the chain of spans that actually determined end-to-end
  latency, with each span's *self time* (time not covered by its children).
* **cross-service edges** - every parent->child hop that crosses a service
  boundary (HTTP calls, Kafka publish->consume), with latency and errors.
* **async gaps** - time a message spent waiting in Kafka between producer end
  and consumer start.
* **baseline deviations** - spans much slower than the same operation in
  healthy traces.

The LLM then reasons over these compact findings instead of raw spans.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from .models import Span, Trace


# ---------------------------------------------------------------------------
# Error analysis
# ---------------------------------------------------------------------------
@dataclass
class ErrorOrigin:
    span: Span
    propagated_through: list[Span]  # ancestors that also erred because of this one

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.span.summary(),
            "propagation_chain": [s.label() for s in self.propagated_through],
            "crossed_services": sorted({s.service for s in self.propagated_through} - {self.span.service}),
        }


def _subtree_has_error(span: Span) -> bool:
    return any(c.is_error or _subtree_has_error(c) for c in span.children)


def error_origins(trace: Trace) -> list[ErrorOrigin]:
    """Find spans that failed on their own (no failing descendant)."""
    origins: list[ErrorOrigin] = []
    for span in trace.spans:
        if not span.is_error or _subtree_has_error(span):
            continue
        chain: list[Span] = []
        parent = trace.parent(span)
        while parent is not None and parent.is_error:
            chain.append(parent)
            parent = trace.parent(parent)
        origins.append(ErrorOrigin(span=span, propagated_through=chain))
    # The earliest failure is usually the trigger; longer propagation = wider blast radius.
    origins.sort(key=lambda o: (o.span.start_us, -len(o.propagated_through)))
    return origins


# ---------------------------------------------------------------------------
# Latency analysis
# ---------------------------------------------------------------------------
def _merge_intervals(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def self_time_us(span: Span) -> int:
    """Span duration not covered by any child (clipped to the span window)."""
    clipped = [
        (max(c.start_us, span.start_us), min(c.end_us, span.end_us))
        for c in span.children
        if c.end_us > span.start_us and c.start_us < span.end_us
    ]
    covered = sum(end - start for start, end in _merge_intervals(clipped))
    return max(0, span.duration_us - covered)


@dataclass
class PathStep:
    span: Span
    self_time_us: int

    def to_dict(self, total_us: int) -> dict[str, Any]:
        return {
            "span": self.span.label(),
            "span_id": self.span.span_id,
            "duration_ms": round(self.span.duration_ms, 2),
            "self_time_ms": round(self.self_time_us / 1000, 2),
            "share_of_trace": round(self.self_time_us / total_us, 3) if total_us else 0.0,
            "error": self.span.is_error,
        }


def critical_path(trace: Trace, root: Span | None = None) -> list[PathStep]:
    """Walk backwards from the end of each span through the last-finishing child.

    Children that overrun their parent (typical for async Kafka consumers) are
    still followed, because their completion is what the user ultimately waits on
    for the order to exist.
    """
    root = root or trace.root
    path: list[PathStep] = []
    node: Span | None = root
    while node is not None:
        path.append(PathStep(node, self_time_us(node)))
        node = max(node.children, key=lambda c: c.end_us, default=None)
    return path


def hotspots(trace: Trace, top: int = 5) -> list[dict[str, Any]]:
    """Spans with the largest exclusive (self) time anywhere in the trace."""
    total = max(1, trace.end_us - trace.start_us)
    ranked = sorted(trace.spans, key=self_time_us, reverse=True)[:top]
    return [
        {
            "span": s.label(),
            "span_id": s.span_id,
            "self_time_ms": round(self_time_us(s) / 1000, 2),
            "share_of_trace": round(self_time_us(s) / total, 3),
            "attributes": s.summary().get("attributes", {}),
        }
        for s in ranked
    ]


# ---------------------------------------------------------------------------
# Service topology within a trace
# ---------------------------------------------------------------------------
@dataclass
class Edge:
    caller: str
    callee: str
    transport: str
    calls: int = 0
    errors: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0
    operations: set[str] = field(default_factory=set)

    def to_dict(self) -> dict[str, Any]:
        return {
            "caller": self.caller,
            "callee": self.callee,
            "transport": self.transport,
            "calls": self.calls,
            "errors": self.errors,
            "avg_ms": round(self.total_ms / self.calls, 2) if self.calls else 0.0,
            "max_ms": round(self.max_ms, 2),
            "operations": sorted(self.operations),
        }


def _transport(parent: Span, child: Span) -> str:
    if "messaging.system" in child.tags or "messaging.system" in parent.tags:
        return str(child.tags.get("messaging.system") or parent.tags.get("messaging.system"))
    if any(k.startswith("http.") for k in child.tags):
        return "http"
    return "rpc"


def service_edges(trace: Trace) -> list[Edge]:
    edges: dict[tuple[str, str, str], Edge] = {}
    for span in trace.spans:
        parent = trace.parent(span)
        if parent is None or parent.service == span.service:
            continue
        transport = _transport(parent, span)
        key = (parent.service, span.service, transport)
        edge = edges.setdefault(key, Edge(parent.service, span.service, transport))
        edge.calls += 1
        edge.errors += int(span.is_error)
        edge.total_ms += span.duration_ms
        edge.max_ms = max(edge.max_ms, span.duration_ms)
        edge.operations.add(span.operation)
    return sorted(edges.values(), key=lambda e: (e.caller, e.callee))


def async_gaps(trace: Trace) -> list[dict[str, Any]]:
    """Queue time between a producer span ending and its consumer starting."""
    gaps = []
    for span in trace.spans:
        if span.kind != "consumer":
            continue
        parent = trace.parent(span)
        if parent is None:
            continue
        gaps.append(
            {
                "producer": parent.label(),
                "consumer": span.label(),
                "topic": span.tags.get("messaging.destination.name"),
                "queue_ms": round(max(0, span.start_us - parent.end_us) / 1000, 2),
                "consumer_error": span.is_error,
            }
        )
    return gaps


# ---------------------------------------------------------------------------
# Baseline comparison
# ---------------------------------------------------------------------------
@dataclass
class OpStats:
    count: int
    p50_ms: float
    p95_ms: float
    error_rate: float


def build_baseline(traces: Iterable[Trace]) -> dict[str, OpStats]:
    """Per-``service:operation`` latency distribution from (ideally healthy) traces."""
    durations: dict[str, list[float]] = defaultdict(list)
    errors: dict[str, int] = defaultdict(int)
    for trace in traces:
        for span in trace.spans:
            durations[span.label()].append(span.duration_ms)
            errors[span.label()] += int(span.is_error)
    baseline = {}
    for label, values in durations.items():
        values.sort()
        p95_idx = min(len(values) - 1, max(0, int(round(0.95 * (len(values) - 1)))))
        baseline[label] = OpStats(
            count=len(values),
            p50_ms=statistics.median(values),
            p95_ms=values[p95_idx],
            error_rate=errors[label] / len(values),
        )
    return baseline


def baseline_deviations(
    trace: Trace, baseline: dict[str, OpStats], slow_factor: float = 2.0, min_samples: int = 3
) -> list[dict[str, Any]]:
    out = []
    for span in trace.spans:
        stats = baseline.get(span.label())
        if stats is None or stats.count < min_samples:
            continue
        reference = max(stats.p95_ms, 1.0)
        ratio = span.duration_ms / reference
        unusual_error = span.is_error and stats.error_rate < 0.05
        if ratio >= slow_factor or unusual_error:
            out.append(
                {
                    "span": span.label(),
                    "span_id": span.span_id,
                    "duration_ms": round(span.duration_ms, 2),
                    "baseline_p50_ms": round(stats.p50_ms, 2),
                    "baseline_p95_ms": round(stats.p95_ms, 2),
                    "slowdown_vs_p95": round(ratio, 2),
                    "error": span.is_error,
                    "baseline_error_rate": round(stats.error_rate, 3),
                    "self_time_ms": round(self_time_us(span) / 1000, 2),
                }
            )
    # Prefer spans whose own work (not children) explains the slowdown.
    out.sort(key=lambda d: (d["error"], d["self_time_ms"] * d["slowdown_vs_p95"]), reverse=True)
    return out


# ---------------------------------------------------------------------------
# Aggregate findings
# ---------------------------------------------------------------------------
@dataclass
class TraceFindings:
    trace: Trace
    origins: list[ErrorOrigin]
    path: list[PathStep]
    edges: list[Edge]
    gaps: list[dict[str, Any]]
    hot: list[dict[str, Any]]
    deviations: list[dict[str, Any]]

    def suspects(self) -> list[dict[str, Any]]:
        """Heuristic ranking of likely root-cause spans, before the LLM weighs in."""
        scores: dict[str, dict[str, Any]] = {}

        def bump(span_id: str, label: str, points: float, reason: str) -> None:
            entry = scores.setdefault(span_id, {"span": label, "span_id": span_id, "score": 0.0, "reasons": []})
            entry["score"] += points
            entry["reasons"].append(reason)

        for i, origin in enumerate(self.origins):
            bump(origin.span.span_id, origin.span.label(), 5.0 - min(i, 3), "error origin (no failing children)")
            if origin.propagated_through:
                bump(
                    origin.span.span_id,
                    origin.span.label(),
                    1.0,
                    f"error propagated through {len(origin.propagated_through)} ancestor span(s)",
                )
        for dev in self.deviations[:5]:
            bump(dev["span_id"], dev["span"], min(4.0, dev["slowdown_vs_p95"]), f"{dev['slowdown_vs_p95']}x slower than baseline p95")
        for step in self.path:
            share = step.self_time_us / max(1, self.trace.end_us - self.trace.start_us)
            if share >= 0.3:
                bump(step.span.span_id, step.span.label(), 3.0 * share, f"{share:.0%} of trace time is its own work")
        return sorted(scores.values(), key=lambda s: s["score"], reverse=True)[:5]


def analyze(
    trace: Trace, baseline: dict[str, OpStats] | None = None, slow_factor: float = 2.0
) -> TraceFindings:
    return TraceFindings(
        trace=trace,
        origins=error_origins(trace),
        path=critical_path(trace),
        edges=service_edges(trace),
        gaps=async_gaps(trace),
        hot=hotspots(trace),
        deviations=baseline_deviations(trace, baseline, slow_factor) if baseline else [],
    )
