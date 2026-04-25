"""Agent loop and toolbox tests - no network, no API key required."""

from types import SimpleNamespace

import httpx
import respx

from tracewhisper.agent import RootCauseAgent, mcp_tools_to_anthropic
from tracewhisper.config import Config
from tracewhisper.toolbox import Toolbox

from .conftest import load_raw

REPORT = {
    "title": "Payment gateway failures break checkout",
    "summary": "The payment gateway is returning connect errors.",
    "root_cause_service": "checkout",
    "root_cause_component": "checkout:payment-gateway charge",
    "category": "dependency_failure",
    "confidence": "high",
    "propagation": ["checkout:payment-gateway charge", "checkout:POST /checkout"],
    "evidence": ["trace a1a1...: gateway span 252ms with upstream connect error"],
    "impact": "~60% of checkouts fail with 502",
    "recommended_actions": ["fail over to the secondary gateway"],
    "trace_ids": ["a1" * 16],
}


def _block(**kw):
    return SimpleNamespace(**kw)


def _response(content, stop_reason):
    return SimpleNamespace(
        content=content, stop_reason=stop_reason, usage=SimpleNamespace(input_tokens=100, output_tokens=20)
    )


class FakeMessages:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append({**kwargs, "messages": list(kwargs["messages"])})
        return self._responses.pop(0)


def _fake_client(responses):
    messages = FakeMessages(responses)
    return SimpleNamespace(beta=SimpleNamespace(messages=messages)), messages


async def test_loop_executes_tools_and_collects_report():
    client, fake = _fake_client(
        [
            _response(
                [
                    _block(type="text", text="Checking recent checkout errors."),
                    _block(type="tool_use", id="tu_1", name="find_traces",
                           input={"service": "checkout", "errors_only": True}),
                ],
                "tool_use",
            ),
            _response([_block(type="tool_use", id="tu_2", name="submit_root_cause", input=REPORT)], "tool_use"),
        ]
    )
    calls = []

    async def call_tool(name, args):
        calls.append((name, args))
        return '{"traces_with_errors": 12}', False

    agent = RootCauseAgent(Config(), client=client)
    inv = await agent.run_loop("investigate", [{"name": "find_traces", "input_schema": {"type": "object"}}], call_tool)

    assert inv.report == REPORT
    assert inv.stopped_because == "report_submitted"
    assert calls == [("find_traces", {"service": "checkout", "errors_only": True})]
    assert inv.turns == 2 and inv.input_tokens == 200

    first, second = fake.calls
    assert first["model"] == "claude-opus-5-5"
    assert first["fallbacks"] == "default"
    assert {t["name"] for t in first["tools"]} == {"find_traces", "submit_root_cause"}
    tool_results = second["messages"][-1]["content"]
    assert tool_results[0]["type"] == "tool_result" and tool_results[0]["tool_use_id"] == "tu_1"


async def test_tool_errors_are_returned_to_the_model():
    client, fake = _fake_client(
        [
            _response([_block(type="tool_use", id="tu_1", name="analyze_trace", input={"trace_id": "x"})], "tool_use"),
            _response([_block(type="text", text="Could not find the trace.")], "end_turn"),
        ]
    )

    async def call_tool(name, args):
        raise RuntimeError("trace x not found")

    inv = await RootCauseAgent(Config(), client=client).run_loop("investigate", [], call_tool)
    assert inv.report is None
    assert inv.final_text == "Could not find the trace."
    result = fake.calls[1]["messages"][-1]["content"][0]
    assert result["is_error"] is True and "not found" in result["content"]


async def test_refusal_stops_the_loop():
    client, _ = _fake_client([_response([], "refusal")])
    inv = await RootCauseAgent(Config(), client=client).run_loop("x", [], None)
    assert inv.stopped_because == "refusal"


def test_mcp_tool_translation():
    tool = SimpleNamespace(name="promql", description="Run PromQL", inputSchema={"type": "object"})
    assert mcp_tools_to_anthropic([tool]) == [
        {"name": "promql", "description": "Run PromQL", "input_schema": {"type": "object"}}
    ]


@respx.mock
async def test_toolbox_analyze_trace_builds_baseline_from_healthy_traces():
    tid = "a1" * 16
    respx.route(method="GET", host="jaeger", path=f"/api/traces/{tid}").mock(
        return_value=httpx.Response(200, json=load_raw("checkout_payment_failure.json"))
    )
    search = respx.route(method="GET", host="jaeger", path="/api/traces").mock(
        return_value=httpx.Response(200, json=load_raw("healthy_checkouts.json"))
    )
    tb = Toolbox(Config(jaeger_url="http://jaeger", prometheus_url="http://prom"))
    try:
        result = await tb.analyze_trace(tid)
    finally:
        await tb.aclose()

    assert search.called
    assert search.calls.last.request.url.params["operation"] == "POST /checkout"
    assert result["baseline"]["healthy_traces"] == 6
    assert result["baseline_deviations"][0]["span"] == "checkout:payment-gateway charge"
    assert result["suspects"][0]["span"] == "checkout:payment-gateway charge"
