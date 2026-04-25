import json

from tracewhisper.correlate import analyze, build_baseline
from tracewhisper.report import findings_to_dict, render_markdown, summarize_traces


def test_findings_are_json_serialisable(payment_failure, healthy):
    data = findings_to_dict(analyze(payment_failure, build_baseline(healthy)))
    encoded = json.dumps(data)
    assert data["overview"]["trace_id"] == payment_failure.trace_id
    assert data["error_origins"][0]["origin"]["operation"] == "payment-gateway charge"
    assert "suspects" in json.loads(encoded)


def test_markdown_mentions_origin_and_path(payment_failure):
    text = render_markdown(analyze(payment_failure))
    assert "### Error origins" in text
    assert "payment-gateway charge" in text
    assert "### Critical path" in text


def test_summary_counts_recurring_origins(payment_failure, orders_dlq, healthy):
    summary = summarize_traces([payment_failure, orders_dlq, *healthy])
    assert summary["traces_examined"] == 8
    assert summary["traces_with_errors"] == 2
    recurring = {r["span"]: r for r in summary["recurring_error_origins"]}
    assert recurring["orders:sqlite write"]["traces"] == 2
    assert "database is locked" in recurring["orders:sqlite write"]["top_messages"]
    assert recurring["checkout:payment-gateway charge"]["traces"] == 1
