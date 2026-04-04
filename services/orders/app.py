"""Orders service: consumes `orders.placed` events and owns the order history."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from aiokafka.structs import ConsumerRecord
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from shopcommon.config import get_settings
from shopcommon.db import Database
from shopcommon.faults import FaultInjector, InjectedFault, admin_router
from shopcommon.metrics import BUSINESS_EVENTS, install_metrics
from shopcommon.telemetry import get_tracer, instrument_app, mark_error
from shopcommon.kafka import EventConsumer, EventProducer

tracer = get_tracer()
log = logging.getLogger("orders")

settings = get_settings()
faults = FaultInjector(known_points=["orders.db.query", "orders.db.write", "orders.process"])
db = Database(settings.db_path, "orders", faults)
dlq = EventProducer(settings.kafka_bootstrap, client_id="orders-dlq")

MAX_ATTEMPTS = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    charge_id TEXT NOT NULL,
    lines_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'confirmed',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
"""


async def persist_order(event: dict) -> None:
    await faults.apply("orders.process")
    await db.execute(
        "INSERT OR IGNORE INTO orders (order_id, customer_id, total_cents, charge_id, lines_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            event["order_id"],
            event["customer_id"],
            event["total_cents"],
            event["charge_id"],
            json.dumps(event["lines"]),
        ),
    )


async def handle_order_placed(event: dict, record: ConsumerRecord) -> None:
    """Persist with bounded retries; poison messages go to the DLQ."""
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with tracer.start_as_current_span(
                "persist order", attributes={"order.id": event.get("order_id", ""), "retry.attempt": attempt}
            ) as span:
                try:
                    await persist_order(event)
                except Exception as exc:
                    mark_error(span, exc)
                    raise
            BUSINESS_EVENTS.labels(settings.service_name, "order_confirmed").inc()
            log.info("order %s stored (offset %s)", event.get("order_id"), record.offset)
            return
        except Exception as exc:  # injected faults, malformed events, sqlite errors
            last_error = exc
            log.warning("attempt %d/%d failed for %s: %s", attempt, MAX_ATTEMPTS, event.get("order_id"), exc)
            await asyncio.sleep(0.1 * 2**attempt)

    await dlq.publish(
        settings.orders_dlq_topic,
        key=event.get("order_id"),
        value={"event": event, "error": str(last_error), "source_offset": record.offset},
    )
    BUSINESS_EVENTS.labels(settings.service_name, "order_dead_lettered").inc()
    log.error("order %s sent to DLQ: %s", event.get("order_id"), last_error)
    raise RuntimeError(f"order {event.get('order_id')} dead-lettered: {last_error}")


consumer = EventConsumer(
    settings.kafka_bootstrap, settings.orders_topic, group_id="orders-service", handler=handle_order_placed
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.executescript(SCHEMA)
    await dlq.start()
    await consumer.start()
    yield
    await consumer.stop()
    await dlq.stop()
    db.close()


app = FastAPI(title="orders", lifespan=lifespan)
instrument_app(app, settings)
install_metrics(app, settings.service_name)
app.include_router(admin_router(faults))


@app.exception_handler(InjectedFault)
async def _fault_handler(_, exc: InjectedFault):
    mark_error(None, exc, **{"fault.point": exc.point})
    log.error("request failed: %s", exc)
    return JSONResponse(status_code=503, content={"detail": str(exc), "fault_point": exc.point})


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "service": settings.service_name}


def _decode(row: dict) -> dict:
    row = dict(row)
    row["lines"] = json.loads(row.pop("lines_json"))
    return row


@app.get("/orders/{order_id}")
async def get_order(order_id: str) -> dict:
    rows = await db.query("SELECT * FROM orders WHERE order_id = ?", (order_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="order not found")
    return _decode(rows[0])


@app.get("/orders")
async def list_orders(customer_id: str, limit: int = 20) -> list[dict]:
    rows = await db.query(
        "SELECT * FROM orders WHERE customer_id = ? ORDER BY created_at DESC LIMIT ?",
        (customer_id, min(limit, 100)),
    )
    return [_decode(r) for r in rows]
