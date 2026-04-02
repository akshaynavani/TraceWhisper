"""Catalog service: product data, prices and inventory reservations."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from shopcommon.config import get_settings
from shopcommon.db import Database
from shopcommon.faults import FaultInjector, InjectedFault, admin_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("catalog")

settings = get_settings()
faults = FaultInjector(
    known_points=["catalog.db.query", "catalog.db.write", "catalog.reserve", "catalog.http.*"]
)
db = Database(settings.db_path, "catalog", faults)

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    stock INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    order_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    PRIMARY KEY (order_id, sku)
);
"""

SEED = [
    ("SKU-1001", "Mechanical Keyboard", 8900, 500),
    ("SKU-1002", "Wireless Mouse", 2900, 800),
    ("SKU-1003", "27in Monitor", 24900, 120),
    ("SKU-1004", "USB-C Hub", 3900, 600),
    ("SKU-1005", "Laptop Stand", 4500, 300),
    ("SKU-1006", "Noise Cancelling Headphones", 19900, 150),
    ("SKU-1007", "Webcam 1080p", 5900, 250),
    ("SKU-1008", "Desk Mat", 1900, 1000),
]


class ReserveItem(BaseModel):
    sku: str
    qty: int = Field(..., gt=0)


class ReserveRequest(BaseModel):
    order_id: str
    items: list[ReserveItem]


class ReleaseRequest(BaseModel):
    order_id: str


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.executescript(SCHEMA)
    await db.execute_many(
        "INSERT OR IGNORE INTO products (sku, name, price_cents, stock) VALUES (?, ?, ?, ?)", SEED
    )
    log.info("catalog ready with %d seeded products", len(SEED))
    yield
    db.close()


app = FastAPI(title="catalog", lifespan=lifespan)
app.include_router(admin_router(faults))


@app.exception_handler(InjectedFault)
async def _fault_handler(_, exc: InjectedFault):
    from fastapi.responses import JSONResponse

    log.error("request failed: %s", exc)
    return JSONResponse(status_code=503, content={"detail": str(exc), "fault_point": exc.point})


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "service": settings.service_name}


@app.get("/products")
async def list_products() -> list[dict]:
    await faults.apply("catalog.http.list_products")
    return await db.query("SELECT sku, name, price_cents, stock FROM products ORDER BY sku")


@app.get("/products/{sku}")
async def get_product(sku: str) -> dict:
    await faults.apply("catalog.http.get_product")
    rows = await db.query("SELECT sku, name, price_cents, stock FROM products WHERE sku = ?", (sku,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"unknown sku {sku}")
    return rows[0]


@app.post("/inventory/reserve")
async def reserve(req: ReserveRequest) -> dict:
    """Atomically reserve stock for every line item or none of them."""
    await faults.apply("catalog.reserve")
    skus = [i.sku for i in req.items]
    placeholders = ",".join("?" for _ in skus)
    rows = await db.query(f"SELECT sku, stock FROM products WHERE sku IN ({placeholders})", skus)
    stock = {r["sku"]: r["stock"] for r in rows}

    shortages = [
        {"sku": i.sku, "requested": i.qty, "available": stock.get(i.sku, 0)}
        for i in req.items
        if stock.get(i.sku, 0) < i.qty
    ]
    if shortages:
        raise HTTPException(status_code=409, detail={"error": "insufficient_stock", "items": shortages})

    for item in req.items:
        await db.execute(
            "UPDATE products SET stock = stock - ? WHERE sku = ? AND stock >= ?",
            (item.qty, item.sku, item.qty),
        )
        await db.execute(
            "INSERT OR REPLACE INTO reservations (order_id, sku, qty) VALUES (?, ?, ?)",
            (req.order_id, item.sku, item.qty),
        )
    return {"order_id": req.order_id, "reserved": [i.model_dump() for i in req.items]}


@app.post("/inventory/release")
async def release(req: ReleaseRequest) -> dict:
    rows = await db.query("SELECT sku, qty FROM reservations WHERE order_id = ?", (req.order_id,))
    for row in rows:
        await db.execute("UPDATE products SET stock = stock + ? WHERE sku = ?", (row["qty"], row["sku"]))
    await db.execute("DELETE FROM reservations WHERE order_id = ?", (req.order_id,))
    return {"order_id": req.order_id, "released": len(rows)}
