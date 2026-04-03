"""Checkout service: prices a cart, reserves stock, charges payment, emits the order event."""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from shopcommon.config import get_settings
from shopcommon.db import Database
from shopcommon.faults import FaultInjector, InjectedFault, admin_router
from shopcommon.kafka import EventProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("checkout")

settings = get_settings()
faults = FaultInjector(
    known_points=["checkout.db.write", "checkout.payment", "checkout.publish", "checkout.pricing"]
)
db = Database(settings.db_path, "checkout", faults)
producer = EventProducer(settings.kafka_bootstrap, client_id="checkout")
http: httpx.AsyncClient | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS checkouts (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    failure_reason TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


class CartItem(BaseModel):
    sku: str
    qty: int = Field(..., gt=0, le=50)


class Payment(BaseModel):
    card_token: str = Field(..., min_length=4)


class CheckoutRequest(BaseModel):
    customer_id: str
    items: list[CartItem] = Field(..., min_length=1)
    payment: Payment


class PaymentDeclined(Exception):
    pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http
    db.executescript(SCHEMA)
    http = httpx.AsyncClient(timeout=settings.http_timeout_s)
    await producer.start()
    yield
    await producer.stop()
    await http.aclose()
    db.close()


app = FastAPI(title="checkout", lifespan=lifespan)
app.include_router(admin_router(faults))


@app.exception_handler(InjectedFault)
async def _fault_handler(_, exc: InjectedFault):
    log.error("request failed: %s", exc)
    return JSONResponse(status_code=503, content={"detail": str(exc), "fault_point": exc.point})


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "service": settings.service_name}


async def price_cart(items: list[CartItem]) -> tuple[list[dict], int]:
    """Look up every SKU concurrently; this fan-out shows up nicely in traces."""
    await faults.apply("checkout.pricing")
    assert http is not None

    async def fetch(item: CartItem) -> dict:
        resp = await http.get(f"{settings.catalog_url}/products/{item.sku}")
        if resp.status_code == 404:
            raise HTTPException(status_code=422, detail=f"unknown sku {item.sku}")
        resp.raise_for_status()
        product = resp.json()
        return {**item.model_dump(), "unit_price_cents": product["price_cents"], "name": product["name"]}

    lines = await asyncio.gather(*(fetch(i) for i in items))
    total = sum(line["unit_price_cents"] * line["qty"] for line in lines)
    return list(lines), total


async def reserve_inventory(order_id: str, items: list[CartItem]) -> None:
    assert http is not None
    resp = await http.post(
        f"{settings.catalog_url}/inventory/reserve",
        json={"order_id": order_id, "items": [i.model_dump() for i in items]},
    )
    if resp.status_code == 409:
        raise HTTPException(status_code=409, detail=resp.json().get("detail"))
    resp.raise_for_status()


async def release_inventory(order_id: str) -> None:
    assert http is not None
    try:
        resp = await http.post(f"{settings.catalog_url}/inventory/release", json={"order_id": order_id})
        resp.raise_for_status()
    except httpx.HTTPError:
        log.exception("failed to release inventory for %s", order_id)


async def charge_payment(order_id: str, payment: Payment, amount_cents: int) -> str:
    """Simulated payment gateway. Card tokens starting with 'decline' are rejected."""
    await faults.apply("checkout.payment")
    await asyncio.sleep(0.04)  # typical gateway round trip
    if payment.card_token.startswith("decline"):
        raise PaymentDeclined("card declined by issuer")
    return f"ch_{uuid.uuid4().hex[:16]}"


@app.post("/checkout", status_code=201)
async def checkout(req: CheckoutRequest) -> dict:
    order_id = f"ord_{uuid.uuid4().hex[:12]}"
    lines, total = await price_cart(req.items)

    await db.execute(
        "INSERT INTO checkouts (order_id, customer_id, total_cents, status) VALUES (?, ?, ?, 'pending')",
        (order_id, req.customer_id, total),
    )

    try:
        await reserve_inventory(order_id, req.items)
    except (httpx.HTTPError, HTTPException) as exc:
        await _mark(order_id, "failed", f"reservation: {exc}")
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=502, detail="catalog unavailable") from exc

    try:
        charge_id = await charge_payment(order_id, req.payment, total)
    except (PaymentDeclined, InjectedFault) as exc:
        await release_inventory(order_id)
        await _mark(order_id, "failed", f"payment: {exc}")
        status = 402 if isinstance(exc, PaymentDeclined) else 502
        raise HTTPException(status_code=status, detail=f"payment failed: {exc}") from exc

    event = {
        "order_id": order_id,
        "customer_id": req.customer_id,
        "lines": lines,
        "total_cents": total,
        "charge_id": charge_id,
    }
    await faults.apply("checkout.publish")
    await producer.publish(settings.orders_topic, key=order_id, value=event)
    await _mark(order_id, "completed", None)
    return {"order_id": order_id, "total_cents": total, "charge_id": charge_id, "status": "accepted"}


async def _mark(order_id: str, status: str, reason: str | None) -> None:
    await db.execute(
        "UPDATE checkouts SET status = ?, failure_reason = ? WHERE order_id = ?",
        (status, reason, order_id),
    )
