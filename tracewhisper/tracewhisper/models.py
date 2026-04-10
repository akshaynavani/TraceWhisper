"""Normalized trace model built from Jaeger's JSON API format."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SpanEvent:
    timestamp_us: int
    name: str
    attributes: dict[str, Any]


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_id: str | None
    service: str
    operation: str
    start_us: int
    duration_us: int
    kind: str = "internal"
    tags: dict[str, Any] = field(default_factory=dict)
    events: list[SpanEvent] = field(default_factory=list)
    children: list["Span"] = field(default_factory=list, repr=False)

    @property
    def end_us(self) -> int:
        return self.start_us + self.duration_us

    @property
    def duration_ms(self) -> float:
        return self.duration_us / 1000.0

    @property
    def is_error(self) -> bool:
        if str(self.tags.get("error", "")).lower() == "true":
            return True
        if str(self.tags.get("otel.status_code", "")).upper() == "ERROR":
            return True
        status = self.http_status
        return status is not None and status >= 500

    @property
    def http_status(self) -> int | None:
        for key in ("http.response.status_code", "http.status_code"):
            if key in self.tags:
                try:
                    return int(self.tags[key])
                except (TypeError, ValueError):
                    return None
        return None

    @property
    def status_message(self) -> str | None:
        return self.tags.get("otel.status_description")

    @property
    def exceptions(self) -> list[dict[str, Any]]:
        out = []
        for ev in self.events:
            if ev.name == "exception" or "exception.type" in ev.attributes:
                out.append(
                    {
                        "type": ev.attributes.get("exception.type"),
                        "message": ev.attributes.get("exception.message"),
                        "stacktrace_tail": _tail(ev.attributes.get("exception.stacktrace")),
                    }
                )
        return out

    def label(self) -> str:
        return f"{self.service}:{self.operation}"

    def summary(self) -> dict[str, Any]:
        """Compact representation suitable for an LLM context window."""
        data: dict[str, Any] = {
            "span_id": self.span_id,
            "service": self.service,
            "operation": self.operation,
            "kind": self.kind,
            "duration_ms": round(self.duration_ms, 2),
            "error": self.is_error,
        }
        if self.http_status is not None:
            data["http_status"] = self.http_status
        if self.status_message:
            data["status_message"] = self.status_message
        if self.exceptions:
            data["exceptions"] = self.exceptions
        interesting = {
            k: v
            for k, v in self.tags.items()
            if k.startswith(("db.", "messaging.", "payment.", "fault.", "peer.", "order.", "retry."))
            or k in ("http.route", "http.method", "http.request.method", "url.full", "http.url")
        }
        if interesting:
            data["attributes"] = interesting
        return data


def _tail(text: str | None, lines: int = 4) -> str | None:
    if not text:
        return None
    return "\n".join(text.strip().splitlines()[-lines:])


@dataclass
class Trace:
    trace_id: str
    spans: list[Span]

    def __post_init__(self) -> None:
        self.by_id: dict[str, Span] = {s.span_id: s for s in self.spans}
        for span in self.spans:
            span.children = []
        for span in self.spans:
            parent = self.by_id.get(span.parent_id) if span.parent_id else None
            if parent is not None:
                parent.children.append(span)
        for span in self.spans:
            span.children.sort(key=lambda c: c.start_us)

    @property
    def roots(self) -> list[Span]:
        return sorted(
            (s for s in self.spans if not s.parent_id or s.parent_id not in self.by_id),
            key=lambda s: s.start_us,
        )

    @property
    def root(self) -> Span:
        return self.roots[0]

    @property
    def services(self) -> list[str]:
        return sorted({s.service for s in self.spans})

    @property
    def start_us(self) -> int:
        return min(s.start_us for s in self.spans)

    @property
    def end_us(self) -> int:
        return max(s.end_us for s in self.spans)

    @property
    def duration_ms(self) -> float:
        return (self.end_us - self.start_us) / 1000.0

    @property
    def has_error(self) -> bool:
        return any(s.is_error for s in self.spans)

    def parent(self, span: Span) -> Span | None:
        return self.by_id.get(span.parent_id) if span.parent_id else None

    def walk(self, span: Span | None = None):
        """Depth-first walk over every root (or the given subtree)."""
        stack = [span] if span else list(reversed(self.roots))
        while stack:
            current = stack.pop()
            yield current
            stack.extend(reversed(current.children))

    # ------------------------------------------------------------------
    @classmethod
    def from_jaeger(cls, data: dict[str, Any]) -> "Trace":
        processes = data.get("processes", {})
        spans: list[Span] = []
        for raw in data.get("spans", []):
            process = processes.get(raw.get("processID"), {})
            tags = {t["key"]: t.get("value") for t in raw.get("tags", [])}
            parent_id = None
            for ref in raw.get("references", []) or []:
                if ref.get("refType") == "CHILD_OF":
                    parent_id = ref.get("spanID")
                    break
            if parent_id is None and raw.get("references"):
                parent_id = raw["references"][0].get("spanID")  # FOLLOWS_FROM
            events = []
            for log in raw.get("logs", []) or []:
                attrs = {f["key"]: f.get("value") for f in log.get("fields", [])}
                events.append(SpanEvent(log.get("timestamp", 0), str(attrs.get("event", "log")), attrs))
            spans.append(
                Span(
                    trace_id=raw["traceID"],
                    span_id=raw["spanID"],
                    parent_id=parent_id,
                    service=process.get("serviceName", "unknown"),
                    operation=raw.get("operationName", ""),
                    start_us=int(raw.get("startTime", 0)),
                    duration_us=int(raw.get("duration", 0)),
                    kind=str(tags.get("span.kind", "internal")),
                    tags=tags,
                    events=events,
                )
            )
        return cls(trace_id=data.get("traceID", spans[0].trace_id if spans else ""), spans=spans)
