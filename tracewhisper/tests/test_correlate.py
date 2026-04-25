from tracewhisper.correlate import (
    analyze,
    async_gaps,
    baseline_deviations,
    build_baseline,
    critical_path,
    error_origins,
    hotspots,
    self_time_us,
    service_edges,
)


def test_error_origin_is_the_failing_leaf_not_the_propagating_root(payment_failure):
    origins = error_origins(payment_failure)
    assert [o.span.span_id for o in origins] == ["c4"]
    assert [s.span_id for s in origins[0].propagated_through] == ["c0"]


def test_multiple_origins_ordered_by_start(orders_dlq):
    origins = error_origins(orders_dlq)
    assert [o.span.span_id for o in origins] == ["o2", "o4"]
    # Propagation stops at the healthy producer span in checkout.
    assert [s.span_id for s in origins[0].propagated_through] == ["o1", "o0"]


def test_self_time_excludes_children(payment_failure):
    root = payment_failure.by_id["c0"]
    # 320ms total minus children covering 30 + 15 + 252 + 6 ms.
    assert self_time_us(root) == 17_000
    assert self_time_us(payment_failure.by_id["c4"]) == 252_000


def test_critical_path_includes_sequential_children(payment_failure):
    path_ids = [step.span.span_id for step in critical_path(payment_failure)]
    assert path_ids[0] == "c0"
    assert "c4" in path_ids and "c5" in path_ids
    assert path_ids.index("c4") < path_ids.index("c5")


def test_critical_path_follows_async_consumer(orders_dlq):
    path_ids = [step.span.span_id for step in critical_path(orders_dlq)]
    assert "o0" in path_ids  # consumer overruns the HTTP request but is still followed


def test_hotspots_rank_by_self_time(payment_failure):
    assert hotspots(payment_failure)[0]["span_id"] == "c4"


def test_service_edges_detect_http_and_kafka(orders_dlq):
    edges = {(e.caller, e.callee, e.transport): e for e in service_edges(orders_dlq)}
    assert ("checkout", "catalog", "http") in edges
    kafka = edges[("checkout", "orders", "kafka")]
    assert kafka.calls == 1 and kafka.errors == 1


def test_async_gap_measures_queue_time(orders_dlq):
    (gap,) = [g for g in async_gaps(orders_dlq) if g["topic"] == "orders.placed"]
    assert gap["queue_ms"] == 40.0
    assert gap["consumer_error"] is True


def test_baseline_flags_slow_failing_gateway(payment_failure, healthy):
    baseline = build_baseline(healthy)
    stats = baseline["checkout:payment-gateway charge"]
    assert stats.count == 6 and stats.p95_ms == 45.0
    deviations = baseline_deviations(payment_failure, baseline, slow_factor=2.0)
    assert deviations[0]["span_id"] == "c4"
    assert deviations[0]["slowdown_vs_p95"] == 5.6


def test_suspects_put_gateway_first(payment_failure, healthy):
    findings = analyze(payment_failure, build_baseline(healthy))
    suspects = findings.suspects()
    assert suspects[0]["span_id"] == "c4"
    assert any("error origin" in r for r in suspects[0]["reasons"])
