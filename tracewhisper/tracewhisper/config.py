"""Runtime configuration for the analyzer, MCP server and agent."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Config:
    jaeger_url: str = field(default_factory=lambda: _env("JAEGER_URL", "http://localhost:16686"))
    prometheus_url: str = field(default_factory=lambda: _env("PROMETHEUS_URL", "http://localhost:9090"))
    http_timeout_s: float = field(default_factory=lambda: float(_env("TRACEWHISPER_HTTP_TIMEOUT_S", "10")))

    # LLM settings. The API key itself is read by the Anthropic SDK from
    # ANTHROPIC_API_KEY (or an `ant auth login` profile) - never stored here.
    model: str = field(default_factory=lambda: _env("TRACEWHISPER_MODEL", "claude-opus-5-5"))
    effort: str = field(default_factory=lambda: _env("TRACEWHISPER_EFFORT", "high"))
    max_agent_turns: int = field(default_factory=lambda: int(_env("TRACEWHISPER_MAX_TURNS", "12")))

    # A span is "slow" when it exceeds baseline p95 by this factor.
    slow_factor: float = field(default_factory=lambda: float(_env("TRACEWHISPER_SLOW_FACTOR", "2.0")))


def load_config() -> Config:
    return Config()
