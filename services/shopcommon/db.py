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
from typing import Any, Iterable

from .faults import FaultInjector


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
        if self.faults:
            self.faults.apply_sync(f"{self.service}.db.{op}")
        with self._lock:
            cur = self._conn.executemany(sql, params) if many else self._conn.execute(sql, tuple(params))
            rows = cur.fetchall() if cur.description else []
            return [dict(r) for r in rows]

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
