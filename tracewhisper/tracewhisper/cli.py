"""Command-line entry point.

    tracewhisper analyze <trace_id>            # deterministic correlation, no LLM
    tracewhisper scan --service checkout       # recurring failures across recent traces
    tracewhisper investigate "checkout 502s"   # full LLM root-cause analysis via MCP
    tracewhisper serve-mcp                     # run the MCP server on stdio
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from .config import load_config
from .correlate import analyze, build_baseline
from .report import render_markdown

console = Console(stderr=False)


async def _analyze(trace_id: str, lookback: str, as_json: bool) -> int:
    from .toolbox import Toolbox

    tb = Toolbox(load_config())
    try:
        if as_json:
            print(json.dumps(await tb.analyze_trace(trace_id, lookback), indent=2, default=str))
            return 0
        trace = await tb.jaeger.get_trace(trace_id)
        root = trace.root
        healthy = [
            t
            for t in await tb.jaeger.find_traces(root.service, operation=root.operation, lookback=lookback, limit=50)
            if not t.has_error and t.trace_id != trace_id
        ]
        findings = analyze(trace, build_baseline(healthy), tb.config.slow_factor)
        console.print(Markdown(render_markdown(findings)))
        return 0
    finally:
        await tb.aclose()


async def _scan(service: str, lookback: str, errors_only: bool, min_ms: int | None) -> int:
    from .toolbox import Toolbox

    tb = Toolbox(load_config())
    try:
        data = await tb.find_traces(service, lookback=lookback, errors_only=errors_only, min_duration_ms=min_ms, limit=50)
        print(json.dumps(data, indent=2, default=str))
        return 0
    finally:
        await tb.aclose()


async def _investigate(symptom: str | None, trace_id: str | None, lookback: str, as_json: bool) -> int:
    from .agent import RootCauseAgent

    agent = RootCauseAgent(load_config())
    with console.status("investigating...") as status:
        inv = await agent.investigate(
            symptom, trace_id, lookback, on_progress=lambda m: console.log(m[:300])
        )
        status.stop()

    if as_json:
        print(json.dumps({"report": inv.report, "final_text": inv.final_text, "tool_calls": inv.tool_calls,
                          "turns": inv.turns, "stopped_because": inv.stopped_because}, indent=2))
        return 0 if inv.report else 1

    if not inv.report:
        console.print(Panel(inv.final_text or "(no answer)", title=f"No structured report ({inv.stopped_because})"))
        return 1
    r = inv.report
    body = [
        f"**{r['summary']}**",
        "",
        f"- **Root cause:** `{r['root_cause_service']}` / `{r['root_cause_component']}` "
        f"({r['category']}, confidence: {r['confidence']})",
        f"- **Propagation:** {' -> '.join(r['propagation'])}",
        f"- **Impact:** {r['impact']}",
        "",
        "**Evidence**",
        *[f"- {e}" for e in r["evidence"]],
        "",
        "**Recommended actions**",
        *[f"1. {a}" for a in r["recommended_actions"]],
        "",
        f"Traces: {', '.join(r['trace_ids']) or '-'}",
    ]
    console.print(Panel(Markdown("\n".join(body)), title=r["title"]))
    console.print(
        f"[dim]{inv.turns} turns, {len(inv.tool_calls)} tool calls, "
        f"{inv.input_tokens} in / {inv.output_tokens} out tokens[/dim]"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tracewhisper", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("analyze", help="correlate a single trace (no LLM)")
    p.add_argument("trace_id")
    p.add_argument("--baseline-lookback", default="1h")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("scan", help="summarise recurring failures for a service")
    p.add_argument("--service", required=True)
    p.add_argument("--lookback", default="15m")
    p.add_argument("--errors-only", action="store_true")
    p.add_argument("--min-ms", type=int)

    p = sub.add_parser("investigate", help="LLM root-cause analysis through the MCP server")
    p.add_argument("symptom", nargs="?", help="free-text description of what is wrong")
    p.add_argument("--trace-id")
    p.add_argument("--lookback", default="15m")
    p.add_argument("--json", action="store_true")

    sub.add_parser("serve-mcp", help="run the MCP server over stdio")

    args = parser.parse_args(argv)
    if args.cmd == "analyze":
        return asyncio.run(_analyze(args.trace_id, args.baseline_lookback, args.json))
    if args.cmd == "scan":
        return asyncio.run(_scan(args.service, args.lookback, args.errors_only, args.min_ms))
    if args.cmd == "investigate":
        return asyncio.run(_investigate(args.symptom, args.trace_id, args.lookback, args.json))
    if args.cmd == "serve-mcp":
        from .mcp_server import main as serve

        serve()
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
