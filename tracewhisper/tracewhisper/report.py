"""Render correlation findings as compact JSON (for the LLM) or Markdown (for humans)."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .correlate import TraceFindings, analyze, self_time_us
from .models import Trace


def trace_overview(trace: Trace) -> dict[str, Any]:
    root = trace.root
    return {
        "trace_id": trace.trace_id,
        "root": root.label(),
        "started_at": datetime.fromtimestamp(trace.start_us / 1e6, tz=timezone.utc).isoformat(),
        "duration_ms": round(trace.duration_ms, 2),
        "span_count": len(trace.spans),
        "services": trace.services,
        "has_error": trace.has_error,
        "http_status": root.http_status,
    }


def findings_to_dict(f: TraceFindings, max_path: int = 12) -> dict[str, Any]:
    total_us = max(1, f.trace.end_us - f.trace.start_us)
    return {
        "overview": trace_overview(f.trace),
        "error_origins": [o.to_dict() for o in f.origins[:5]],
        "critical_path": [s.to_dict(total_us) for s in f.path[:max_path]],
        "hotspots": f.hot,
        "service_edges": [e.to_dict() for e in f.edges],
        "async_gaps": f.gaps,
        "baseline_deviations": f.deviations[:8],
        "suspects": f.suspects(),
    }


def summarize_traces(traces: list[Trace]) -> dict[str, Any]:
    """Cross-trace view: which failure origins / slow spans recur across a sample."""
    origin_counts: Counter[str] = Counter()
    origin_messages: dict[str, Counter[str]] = {}
    slowest_self: Counter[str] = Counter()
    for trace in traces:
        f = analyze(trace)
        for o in f.origins:
            label = o.span.label()
            origin_counts[label] += 1
            msg = o.span.status_message or (o.span.exceptions[0]["message"] if o.span.exceptions else None)
            if msg:
                origin_messages.setdefault(label, Counter())[str(msg)[:160]] += 1
        if trace.spans:
            top = max(trace.spans, key=self_time_us)
            slowest_self[top.label()] += 1

    error_traces = sum(1 for t in traces if t.has_error)
    durations = sorted(t.duration_ms for t in traces)
    return {
        "traces_examined": len(traces),
        "traces_with_errors": error_traces,
        "duration_ms": {
            "min": round(durations[0], 2) if durations else None,
            "median": round(durations[len(durations) // 2], 2) if durations else None,
            "max": round(durations[-1], 2) if durations else None,
        },
        "recurring_error_origins": [
            {
                "span": label,
                "traces": count,
                "top_messages": [m for m, _ in origin_messages.get(label, Counter()).most_common(3)],
            }
            for label, count in origin_counts.most_common(5)
        ],
        "most_common_dominant_span": [
            {"span": label, "traces": count} for label, count in slowest_self.most_common(5)
        ],
        "sample": [trace_overview(t) for t in traces[:10]],
    }


def render_markdown(f: TraceFindings) -> str:
    d = findings_to_dict(f)
    ov = d["overview"]
    lines = [
        f"## Trace `{ov['trace_id']}`",
        f"- root: **{ov['root']}** ({ov['duration_ms']} ms, {ov['span_count']} spans, "
        f"services: {', '.join(ov['services'])})",
        f"- status: {'ERROR' if ov['has_error'] else 'ok'}",
        "",
    ]
    if d["error_origins"]:
        lines.append("### Error origins")
        for o in d["error_origins"]:
            origin = o["origin"]
            msg = origin.get("status_message") or ""
            chain = " <- ".join(o["propagation_chain"]) or "(not propagated)"
            lines.append(f"- `{origin['service']}:{origin['operation']}` {msg}  \n  propagated: {chain}")
        lines.append("")
    lines.append("### Critical path")
    for step in d["critical_path"]:
        flag = " (error)" if step["error"] else ""
        lines.append(
            f"- {step['span']} - {step['duration_ms']} ms total, {step['self_time_ms']} ms self "
            f"({step['share_of_trace']:.0%}){flag}"
        )
    if d["baseline_deviations"]:
        lines += ["", "### Slower than baseline"]
        for dev in d["baseline_deviations"]:
            lines.append(
                f"- {dev['span']}: {dev['duration_ms']} ms vs p95 {dev['baseline_p95_ms']} ms "
                f"({dev['slowdown_vs_p95']}x)"
            )
    if d["async_gaps"]:
        lines += ["", "### Kafka hops"]
        for gap in d["async_gaps"]:
            lines.append(f"- {gap['producer']} -> {gap['consumer']} queued {gap['queue_ms']} ms")
    if d["suspects"]:
        lines += ["", "### Heuristic suspects"]
        for s in d["suspects"]:
            lines.append(f"- **{s['span']}** (score {s['score']:.1f}): {'; '.join(s['reasons'])}")
    return "\n".join(lines)
