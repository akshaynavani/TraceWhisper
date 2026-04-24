"""Chaos scenarios: arm a fault, drive load, and print traces to investigate.

    python -m loadgen.chaos list
    python -m loadgen.chaos run slow-catalog-db --duration 60
    python -m loadgen.chaos run payment-outage --investigate
    python -m loadgen.chaos clear

Each scenario is a known ground truth, which makes it easy to judge whether
TraceWhisper's root-cause summary is right.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
from dataclasses import dataclass

import httpx

from .load import print_summary, run

SERVICES = {
    "catalog": os.getenv("CATALOG_URL", "http://localhost:8001"),
    "checkout": os.getenv("CHECKOUT_URL", "http://localhost:8002"),
    "orders": os.getenv("ORDERS_URL", "http://localhost:8003"),
}


@dataclass(frozen=True)
class Scenario:
    name: str
    service: str
    fault: dict
    symptom: str
    ground_truth: str


SCENARIOS = {
    s.name: s
    for s in [
        Scenario(
            "slow-catalog-db",
            "catalog",
            {"point": "catalog.db.query", "latency_ms": 900, "jitter_ms": 300},
            "Checkout latency has spiked and some checkouts time out.",
            "SQLite reads in catalog are slow; checkout's concurrent pricing calls wait on them "
            "and the HTTP client timeout turns some into 502s.",
        ),
        Scenario(
            "payment-outage",
            "checkout",
            {
                "point": "checkout.payment",
                "error_rate": 0.6,
                "latency_ms": 250,
                "error_message": "payment gateway: upstream connect error or disconnect/reset before headers",
            },
            "Customers report failed checkouts with 502 errors.",
            "The external payment gateway is failing; checkout releases inventory and returns 502.",
        ),
        Scenario(
            "catalog-pool-exhausted",
            "catalog",
            {
                "point": "catalog.reserve",
                "error_rate": 0.5,
                "error_message": "QueuePool limit reached, connection timed out",
            },
            "Intermittent checkout failures, payment metrics look normal.",
            "Catalog's inventory reservation endpoint is failing (503) and checkout surfaces it as 502.",
        ),
        Scenario(
            "orders-db-locked",
            "orders",
            {"point": "orders.db.write", "error_rate": 1.0, "error_message": "database is locked"},
            "Checkouts succeed but orders never show up in order history.",
            "The orders consumer cannot write to SQLite; messages exhaust retries and go to the DLQ. "
            "Visible only after the Kafka hop.",
        ),
    ]
}


async def clear_all() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in SERVICES.items():
            try:
                await client.delete(f"{url}/admin/faults")
                print(f"cleared faults on {name}")
            except httpx.HTTPError as exc:
                print(f"could not reach {name}: {exc}")


async def arm(scenario: Scenario, duration: float) -> None:
    url = SERVICES[scenario.service]
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(f"{url}/admin/faults", json={**scenario.fault, "duration_s": duration + 30})
        resp.raise_for_status()
    print(f"armed {scenario.fault['point']} on {scenario.service}")


async def run_scenario(scenario: Scenario, rps: float, duration: float, warmup: float) -> list[str]:
    if warmup > 0:
        print(f"warm-up: {warmup:.0f}s of healthy traffic to build a baseline")
        await run(SERVICES["checkout"], SERVICES["catalog"], rps, warmup, None, 1000)
    await arm(scenario, duration)
    stats = await run(SERVICES["checkout"], SERVICES["catalog"], rps, duration, None, 1000)
    print_summary(stats)
    return [t for _, t in stats.failed_traces] + [t for _, t in sorted(stats.slow_traces, reverse=True)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("clear")
    p = sub.add_parser("run")
    p.add_argument("scenario", choices=sorted(SCENARIOS))
    p.add_argument("--rps", type=float, default=8.0)
    p.add_argument("--duration", type=float, default=45.0)
    p.add_argument("--warmup", type=float, default=30.0)
    p.add_argument("--investigate", action="store_true", help="run `tracewhisper investigate` afterwards")
    p.add_argument("--keep", action="store_true", help="leave the fault armed afterwards")
    args = parser.parse_args()

    if args.cmd == "list":
        for s in SCENARIOS.values():
            print(f"{s.name:24} [{s.service}] {s.symptom}\n{'':24} truth: {s.ground_truth}\n")
        return
    if args.cmd == "clear":
        asyncio.run(clear_all())
        return

    scenario = SCENARIOS[args.scenario]
    traces = asyncio.run(run_scenario(scenario, args.rps, args.duration, args.warmup))
    if not args.keep:
        asyncio.run(clear_all())
    print(f"\nground truth: {scenario.ground_truth}")
    if args.investigate:
        cmd = [sys.executable, "-m", "tracewhisper.cli", "investigate", scenario.symptom, "--lookback", "10m"]
        if traces:
            cmd += ["--trace-id", traces[0]]
        print("running:", " ".join(cmd))
        subprocess.run(cmd, check=False)
    elif traces:
        print(f"try: tracewhisper analyze {traces[0]}")


if __name__ == "__main__":
    main()
