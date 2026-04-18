"""Prompts and the structured-report tool for the root-cause agent."""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are TraceWhisper, an on-call SRE assistant performing root-cause analysis for a \
microservice e-commerce system.

System under investigation:
- catalog (FastAPI + SQLite): product lookups, inventory reserve/release.
- checkout (FastAPI + SQLite): prices the cart by calling catalog concurrently, reserves stock, \
charges an external payment gateway (span "payment-gateway charge"), then publishes to Kafka topic \
"orders.placed".
- orders (FastAPI + SQLite): consumes "orders.placed", persists orders with up to 3 retries, \
dead-letters to "orders.placed.dlq".
All services export OpenTelemetry traces to Jaeger; trace context crosses the Kafka hop via \
message headers, so one trace covers checkout -> catalog -> Kafka -> orders. Prometheus has RED \
metrics, SQLite latency, Kafka throughput and business events.

How to investigate:
1. Establish the symptom with metrics (service_metrics) and recent traces (find_traces, \
errors_only or min_duration_ms as appropriate). If you were given a trace id, start there.
2. Analyze 1-3 representative traces with analyze_trace. Distinguish the *error origin* (the \
deepest span that failed on its own) from spans that merely propagated the error, and the span \
whose *self time* dominates latency from parents that were just waiting on it.
3. Check the hypothesis against a second signal: another trace, a metric, or span_details. \
Prefer evidence that crosses a service boundary (e.g. checkout 502 caused by catalog 503 caused by \
a slow SQLite statement).
4. Stop once the evidence is consistent; do not keep calling tools for completeness.

When you are done, call submit_root_cause exactly once. Cite concrete evidence: trace ids, \
span names, durations, error messages, metric values. If the evidence is inconclusive, say so \
and lower confidence rather than guessing.
"""

SUBMIT_TOOL = {
    "name": "submit_root_cause",
    "description": (
        "Submit the final root-cause analysis. Call this once, after the investigation, "
        "with evidence drawn from the tool results."
    ),
    "strict": True,
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "title": {"type": "string", "description": "One-line incident title."},
            "summary": {"type": "string", "description": "2-4 sentence plain-language explanation."},
            "root_cause_service": {"type": "string"},
            "root_cause_component": {
                "type": "string",
                "description": "Span/operation or dependency at fault, e.g. 'catalog:sqlite query'.",
            },
            "category": {
                "type": "string",
                "enum": [
                    "dependency_failure",
                    "database_latency",
                    "resource_exhaustion",
                    "messaging_failure",
                    "application_error",
                    "upstream_latency",
                    "bad_input",
                    "unknown",
                ],
            },
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
            "propagation": {
                "type": "array",
                "items": {"type": "string"},
                "description": "How the failure travelled, origin first, e.g. ['catalog:sqlite query', 'catalog:POST /inventory/reserve', 'checkout:POST /checkout'].",
            },
            "evidence": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Concrete observations with trace ids / metric values.",
            },
            "impact": {"type": "string", "description": "User-visible effect and scope."},
            "recommended_actions": {"type": "array", "items": {"type": "string"}},
            "trace_ids": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "title",
            "summary",
            "root_cause_service",
            "root_cause_component",
            "category",
            "confidence",
            "propagation",
            "evidence",
            "impact",
            "recommended_actions",
            "trace_ids",
        ],
    },
}


def build_task(symptom: str | None, trace_id: str | None, lookback: str) -> str:
    parts = ["Investigate the following incident and determine the root cause."]
    if symptom:
        parts.append(f"Reported symptom: {symptom}")
    if trace_id:
        parts.append(f"Example failing/slow trace id: {trace_id}")
    if not symptom and not trace_id:
        parts.append("No specific report - look for the most significant current problem.")
    parts.append(f"Focus on telemetry from the last {lookback}.")
    return "\n".join(parts)
