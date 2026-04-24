"""Async load generator for the demo shop.

    python -m loadgen.load --rps 10 --duration 60

Prints a summary and the trace ids (from the X-Trace-Id response header) of
failed or slow requests so they can be handed to `tracewhisper analyze`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import time
from collections import Counter
from dataclasses import dataclass, field

import httpx

SKUS = [f"SKU-100{i}" for i in range(1, 9)]


@dataclass
class Stats:
    latencies_ms: list[float] = field(default_factory=list)
    statuses: Counter = field(default_factory=Counter)
    failed_traces: list[tuple[int, str]] = field(default_factory=list)
    slow_traces: list[tuple[float, str]] = field(default_factory=list)


def random_cart(rng: random.Random) -> dict:
    items = rng.sample(SKUS, k=rng.randint(1, 4))
    token = "decline-0000" if rng.random() < 0.03 else f"tok_{rng.randrange(10**8):08d}"
    return {
        "customer_id": f"cust-{rng.randint(1, 200)}",
        "items": [{"sku": s, "qty": rng.randint(1, 2)} for s in items],
        "payment": {"card_token": token},
    }


async def one_request(client: httpx.AsyncClient, base: str, catalog: str, rng: random.Random, stats: Stats, slow_ms: float):
    start = time.perf_counter()
    try:
        if rng.random() < 0.8:
            resp = await client.post(f"{base}/checkout", json=random_cart(rng))
        else:
            resp = await client.get(f"{catalog}/products/{rng.choice(SKUS)}")
        status = resp.status_code
        trace_id = resp.headers.get("x-trace-id", "")
    except httpx.HTTPError as exc:
        status, trace_id = 599, ""
        stats.statuses[f"client:{type(exc).__name__}"] += 1
    elapsed = (time.perf_counter() - start) * 1000
    stats.latencies_ms.append(elapsed)
    stats.statuses[status] += 1
    if status >= 500 and trace_id:
        stats.failed_traces.append((status, trace_id))
    elif elapsed > slow_ms and trace_id:
        stats.slow_traces.append((elapsed, trace_id))


async def run(base: str, catalog: str, rps: float, duration: float, seed: int | None, slow_ms: float) -> Stats:
    rng = random.Random(seed)
    stats = Stats()
    interval = 1.0 / rps
    deadline = time.monotonic() + duration
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = []
        while time.monotonic() < deadline:
            tasks.append(asyncio.create_task(one_request(client, base, catalog, rng, stats, slow_ms)))
            await asyncio.sleep(rng.expovariate(1.0 / interval))  # Poisson arrivals
        await asyncio.gather(*tasks)
    return stats


def print_summary(stats: Stats) -> None:
    lat = sorted(stats.latencies_ms)
    if not lat:
        print("no requests sent")
        return
    p = lambda q: lat[min(len(lat) - 1, int(q * len(lat)))]  # noqa: E731
    print(f"requests: {len(lat)}  p50={p(0.5):.0f}ms  p95={p(0.95):.0f}ms  p99={p(0.99):.0f}ms  "
          f"mean={statistics.mean(lat):.0f}ms")
    print("statuses:", dict(stats.statuses))
    for status, tid in stats.failed_traces[:5]:
        print(f"  failed  {status}  trace {tid}")
    for ms, tid in sorted(stats.slow_traces, reverse=True)[:5]:
        print(f"  slow    {ms:.0f}ms trace {tid}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--checkout-url", default=os.getenv("CHECKOUT_URL", "http://localhost:8002"))
    parser.add_argument("--catalog-url", default=os.getenv("CATALOG_URL", "http://localhost:8001"))
    parser.add_argument("--rps", type=float, default=5.0)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--slow-ms", type=float, default=1000.0)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    stats = asyncio.run(run(args.checkout_url, args.catalog_url, args.rps, args.duration, args.seed, args.slow_ms))
    print_summary(stats)


if __name__ == "__main__":
    main()
