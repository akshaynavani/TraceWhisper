"""Tiny async-friendly SQLite helper.

Each service owns its own SQLite file. Queries run in a worker thread so the
event loop never blocks; a fault point named ``<service>.db.<op>`` is checked
before each statement so slow-database incidents can be simulated.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time
from typing import Any, Iterable

from opentelemetry import trace
from opentelemetry.trace import SpanKind

from .faults import FaultInjector
from .metrics import DB_LATENCY
from .telemetry import mark_error

_tracer = trace.get_tracer("tracewhisper.shop.db")


class Database:
    def __init__(self, path: str, service: str, faults: FaultInjector | None = None):
        self.path = path
        self.service = service
        self.faults = faults
        self._lock = threading.Lock()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")

    def _run(self, op: str, sql: str, params: Iterable[Any], many: bool) -> list[dict]:
        # asyncio.to_thread copies contextvars, so this span parents correctly
        # under the request span that awaited us.
        statement = " ".join(sql.split())
        with _tracer.start_as_current_span(
            f"sqlite {op}",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "sqlite",
                "db.name": os.path.basename(self.path),
                "db.operation": statement.split(" ", 1)[0].upper(),
                "db.statement": statement[:500],
            },
        ) as span:
            start = time.perf_counter()
            try:
                if self.faults:
                    self.faults.apply_sync(f"{self.service}.db.{op}")
                lock_start = time.perf_counter()
                with self._lock:
                    lock_wait = time.perf_counter() - lock_start
                    span.set_attribute("db.lock_wait_ms", round(lock_wait * 1000, 2))
                    if many:
                        cur = self._conn.executemany(sql, params)
                    else:
                        cur = self._conn.execute(sql, tuple(params))
                    rows = cur.fetchall() if cur.description else []
                    span.set_attribute("db.rows", len(rows) if rows else max(cur.rowcount, 0))
                    return [dict(r) for r in rows]
            except Exception as exc:
                mark_error(span, exc)
                raise
            finally:
                DB_LATENCY.labels(self.service, op).observe(time.perf_counter() - start)

    async def query(self, sql: str, params: Iterable[Any] = (), op: str = "query") -> list[dict]:
        return await asyncio.to_thread(self._run, op, sql, params, False)

    async def execute(self, sql: str, params: Iterable[Any] = (), op: str = "write") -> list[dict]:
        return await asyncio.to_thread(self._run, op, sql, params, False)

    async def execute_many(self, sql: str, rows: Iterable[Iterable[Any]], op: str = "write") -> None:
        await asyncio.to_thread(self._run, op, sql, list(rows), True)

    def executescript(self, script: str) -> None:
        with self._lock:
            self._conn.executescript(script)

    def close(self) -> None:
        self._conn.close()
