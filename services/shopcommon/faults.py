"""Runtime fault injection.

Each service declares named *fault points* (e.g. ``catalog.db.query`` or
``checkout.payment``). An operator (or the chaos runner) arms faults at runtime
through ``POST /admin/faults``; the service then adds latency or raises errors
whenever execution passes through a matching point. This gives the root-cause
analyzer realistic, reproducible incidents to investigate.
"""

from __future__ import annotations

import asyncio
import fnmatch
import random
import time
from dataclasses import asdict, dataclass, field

from fastapi import APIRouter
from pydantic import BaseModel, Field


class InjectedFault(RuntimeError):
    """Raised when an armed fault decides the current call should fail."""

    def __init__(self, point: str, message: str):
        super().__init__(f"[fault:{point}] {message}")
        self.point = point


@dataclass
class Fault:
    point: str  # glob pattern matched against fault point names
    latency_ms: int = 0
    jitter_ms: int = 0
    error_rate: float = 0.0
    error_message: str = "injected failure"
    expires_at: float | None = None
    hits: int = field(default=0)

    def active(self, now: float) -> bool:
        return self.expires_at is None or now < self.expires_at


class FaultSpec(BaseModel):
    point: str = Field(..., description="Fault point name or glob, e.g. 'catalog.db.*'")
    latency_ms: int = Field(0, ge=0, le=60_000)
    jitter_ms: int = Field(0, ge=0, le=60_000)
    error_rate: float = Field(0.0, ge=0.0, le=1.0)
    error_message: str = "injected failure"
    duration_s: float | None = Field(None, gt=0, description="Auto-disarm after N seconds")


class FaultInjector:
    def __init__(self, known_points: list[str] | None = None, rng: random.Random | None = None):
        self._faults: list[Fault] = []
        self.known_points = sorted(known_points or [])
        self._rng = rng or random.Random()

    # -- management -------------------------------------------------------
    def arm(self, spec: FaultSpec) -> Fault:
        expires = time.time() + spec.duration_s if spec.duration_s else None
        fault = Fault(
            point=spec.point,
            latency_ms=spec.latency_ms,
            jitter_ms=spec.jitter_ms,
            error_rate=spec.error_rate,
            error_message=spec.error_message,
            expires_at=expires,
        )
        self._faults = [f for f in self._faults if f.point != spec.point] + [fault]
        return fault

    def clear(self) -> None:
        self._faults.clear()

    def list(self) -> list[Fault]:
        now = time.time()
        self._faults = [f for f in self._faults if f.active(now)]
        return list(self._faults)

    # -- runtime ----------------------------------------------------------
    def _matching(self, point: str) -> list[Fault]:
        return [f for f in self.list() if fnmatch.fnmatch(point, f.point)]

    def _delay_for(self, fault: Fault) -> float:
        jitter = self._rng.uniform(-fault.jitter_ms, fault.jitter_ms) if fault.jitter_ms else 0
        return max(0.0, (fault.latency_ms + jitter) / 1000.0)

    async def apply(self, point: str) -> None:
        for fault in self._matching(point):
            fault.hits += 1
            delay = self._delay_for(fault)
            if delay:
                await asyncio.sleep(delay)
            if fault.error_rate and self._rng.random() < fault.error_rate:
                raise InjectedFault(point, fault.error_message)

    def apply_sync(self, point: str) -> None:
        """Blocking variant for code already running in a worker thread."""
        for fault in self._matching(point):
            fault.hits += 1
            delay = self._delay_for(fault)
            if delay:
                time.sleep(delay)
            if fault.error_rate and self._rng.random() < fault.error_rate:
                raise InjectedFault(point, fault.error_message)


def admin_router(injector: FaultInjector) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.get("/faults")
    def list_faults() -> dict:
        return {
            "known_points": injector.known_points,
            "armed": [asdict(f) for f in injector.list()],
        }

    @router.post("/faults")
    def arm_fault(spec: FaultSpec) -> dict:
        return asdict(injector.arm(spec))

    @router.delete("/faults")
    def clear_faults() -> dict:
        injector.clear()
        return {"cleared": True}

    return router
