from tracewhisper.models import Trace


def test_parses_jaeger_trace(payment_failure: Trace):
    assert len(payment_failure.spans) == 9
    assert payment_failure.services == ["catalog", "checkout"]
    root = payment_failure.root
    assert root.operation == "POST /checkout"
    assert root.http_status == 502
    assert root.is_error


def test_builds_parent_child_links(payment_failure: Trace):
    by_id = payment_failure.by_id
    assert by_id["k1"].parent_id == "k0"
    assert [c.span_id for c in by_id["c0"].children] == ["c1", "c3", "c4", "c5"]
    assert payment_failure.parent(by_id["k0"]).span_id == "c2"


def test_exceptions_are_extracted_from_logs(payment_failure: Trace):
    gateway = payment_failure.by_id["c4"]
    assert gateway.is_error
    (exc,) = gateway.exceptions
    assert exc["type"].endswith("InjectedFault")
    assert "upstream connect error" in exc["message"]
    assert exc["stacktrace_tail"].splitlines()[-1].startswith("InjectedFault")


def test_summary_keeps_only_interesting_attributes(payment_failure: Trace):
    summary = payment_failure.by_id["c4"].summary()
    assert summary["attributes"] == {"peer.service": "payment-gateway"}
    assert summary["duration_ms"] == 252.0


def test_walk_visits_every_span_once(orders_dlq: Trace):
    ids = [s.span_id for s in orders_dlq.walk()]
    assert sorted(ids) == sorted(s.span_id for s in orders_dlq.spans)
    assert ids[0] == "c0"


def test_empty_references_make_a_root():
    trace = Trace.from_jaeger(
        {
            "traceID": "t",
            "spans": [
                {"traceID": "t", "spanID": "x", "operationName": "op", "references": [],
                 "startTime": 1, "duration": 5, "tags": [], "logs": [], "processID": "p"}
            ],
            "processes": {"p": {"serviceName": "svc"}},
        }
    )
    assert trace.root.span_id == "x"
    assert not trace.has_error
