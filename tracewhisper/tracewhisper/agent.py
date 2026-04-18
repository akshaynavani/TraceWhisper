"""Claude-driven root-cause agent that investigates through the TraceWhisper MCP server.

The agent spawns the MCP server as a stdio subprocess, advertises its tools to
Claude alongside a local ``submit_root_cause`` tool, and runs a manual tool-use
loop until Claude submits a structured report (or the turn budget runs out).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import anthropic
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .config import Config
from .prompts import SUBMIT_TOOL, SYSTEM_PROMPT, build_task

log = logging.getLogger(__name__)

MAX_TOOL_RESULT_CHARS = 60_000
FALLBACK_BETA = "server-side-fallback-2026-07-01"

ToolCaller = Callable[[str, dict[str, Any]], Awaitable[tuple[str, bool]]]
ProgressHook = Callable[[str], None]


@dataclass
class Investigation:
    report: dict[str, Any] | None = None
    final_text: str = ""
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    turns: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    stopped_because: str = ""


def mcp_tools_to_anthropic(mcp_tools: list[Any]) -> list[dict[str, Any]]:
    """Translate MCP tool listings into Messages API tool definitions."""
    return [
        {
            "name": t.name,
            "description": t.description or "",
            "input_schema": t.inputSchema or {"type": "object", "properties": {}},
        }
        for t in mcp_tools
    ]


class RootCauseAgent:
    def __init__(self, config: Config, client: anthropic.AsyncAnthropic | None = None):
        self.config = config
        # Credentials come from ANTHROPIC_API_KEY or an `ant auth login` profile.
        self.client = client or anthropic.AsyncAnthropic()

    # ------------------------------------------------------------------
    async def investigate(
        self,
        symptom: str | None = None,
        trace_id: str | None = None,
        lookback: str = "15m",
        on_progress: ProgressHook | None = None,
    ) -> Investigation:
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "tracewhisper.mcp_server"],
            env={**os.environ},
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                listed = await session.list_tools()
                tools = mcp_tools_to_anthropic(listed.tools)

                async def call_mcp(name: str, args: dict[str, Any]) -> tuple[str, bool]:
                    result = await session.call_tool(name, args)
                    text = "\n".join(c.text for c in result.content if getattr(c, "type", "") == "text")
                    return text, bool(result.isError)

                return await self.run_loop(
                    build_task(symptom, trace_id, lookback), tools, call_mcp, on_progress
                )

    # ------------------------------------------------------------------
    async def run_loop(
        self,
        task: str,
        tools: list[dict[str, Any]],
        call_tool: ToolCaller,
        on_progress: ProgressHook | None = None,
    ) -> Investigation:
        """Manual agentic loop; separated from MCP plumbing so it can be tested with fakes."""
        notify = on_progress or (lambda _msg: None)
        inv = Investigation()
        all_tools = [*tools, SUBMIT_TOOL]
        messages: list[dict[str, Any]] = [{"role": "user", "content": task}]

        for turn in range(1, self.config.max_agent_turns + 1):
            inv.turns = turn
            response = await self._create(messages, all_tools)
            inv.input_tokens += response.usage.input_tokens
            inv.output_tokens += response.usage.output_tokens

            if response.stop_reason == "refusal":
                inv.stopped_because = "refusal"
                return inv

            # Keep the full content (including thinking blocks) so the history stays valid.
            messages.append({"role": "assistant", "content": response.content})
            text = "".join(b.text for b in response.content if b.type == "text").strip()
            if text:
                inv.final_text = text
                notify(text)

            tool_uses = [b for b in response.content if b.type == "tool_use"]
            if not tool_uses:
                inv.stopped_because = response.stop_reason or "end_turn"
                return inv

            results = await asyncio.gather(*(self._execute(tu, call_tool, inv, notify) for tu in tool_uses))
            # All tool results go back in ONE user message.
            messages.append({"role": "user", "content": list(results)})

            if inv.report is not None:
                inv.stopped_because = "report_submitted"
                return inv

            if response.stop_reason == "max_tokens":
                messages.append(
                    {"role": "user", "content": "You hit the output limit; continue concisely."}
                )

        inv.stopped_because = "turn_budget_exhausted"
        return inv

    async def _create(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]):
        return await self.client.beta.messages.create(
            model=self.config.model,
            max_tokens=16000,
            system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            tools=tools,
            messages=messages,
            thinking={"type": "adaptive"},
            output_config={"effort": self.config.effort},
            # On a safety-classifier decline, let the API re-run the request on its
            # recommended fallback model instead of failing the investigation.
            betas=[FALLBACK_BETA],
            fallbacks="default",
        )

    async def _execute(
        self, tool_use: Any, call_tool: ToolCaller, inv: Investigation, notify: ProgressHook
    ) -> dict[str, Any]:
        args = dict(tool_use.input or {})
        if tool_use.name == SUBMIT_TOOL["name"]:
            missing = [k for k in SUBMIT_TOOL["input_schema"]["required"] if k not in args]
            if missing:
                return _tool_result(tool_use.id, f"Missing required fields: {missing}", True)
            inv.report = args
            notify("root-cause report submitted")
            return _tool_result(tool_use.id, "Report recorded.", False)

        notify(f"-> {tool_use.name}({_brief(args)})")
        inv.tool_calls.append({"tool": tool_use.name, "input": args})
        try:
            text, is_error = await call_tool(tool_use.name, args)
        except Exception as exc:  # surface tool failures to the model instead of crashing
            log.exception("tool %s failed", tool_use.name)
            text, is_error = f"{type(exc).__name__}: {exc}", True
        if len(text) > MAX_TOOL_RESULT_CHARS:
            text = text[:MAX_TOOL_RESULT_CHARS] + "\n...[truncated by TraceWhisper; narrow the query]"
        return _tool_result(tool_use.id, text or "(empty result)", is_error)


def _tool_result(tool_use_id: str, content: str, is_error: bool) -> dict[str, Any]:
    block: dict[str, Any] = {"type": "tool_result", "tool_use_id": tool_use_id, "content": content}
    if is_error:
        block["is_error"] = True
    return block


def _brief(args: dict[str, Any], limit: int = 80) -> str:
    text = ", ".join(f"{k}={v!r}" for k, v in args.items())
    return text if len(text) <= limit else text[: limit - 3] + "..."
