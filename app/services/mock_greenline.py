"""
VeriLeaf — Mock Greenline Data Factory

Generates realistic test fixtures matching Greenline API response shapes.
Use for: unit tests, local development, demo environments.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from datetime import datetime, date, timedelta, UTC
import random

from app.models.schemas import (
    GreenlineSaleWebhook,
    GreenlineLineItem,
    GreenlineInventorySnapshot,
    GreenlineInventoryItem,
    EventType,
    ProductCategory,
)


# ── Product Catalogue (realistic Canadian cannabis SKUs) ────────────────────

MOCK_PRODUCTS = [
    {
        "product_id": "GL-001",
        "sku": "REDECAN-WAPPA-3.5G",
        "product_name": "Redecan Wappa 3.5g",
        "category": ProductCategory.DRIED,
        "net_weight_g": Decimal("3.5"),
    },
    {
        "product_id": "GL-002",
        "sku": "BKKT-ICC-28G",
        "product_name": "Back Forty Ice Cream Cake 28g",
        "category": ProductCategory.DRIED,
        "net_weight_g": Decimal("28.0"),
    },
    {
        "product_id": "GL-003",
        "sku": "GH-FULLSPEC-1G",
        "product_name": "General Admission Full Spectrum Oil 1g",
        "category": ProductCategory.EXTRACT,
        "net_weight_g": Decimal("1.0"),
    },
    {
        "product_id": "GL-004",
        "sku": "SHRED-GUMMY-SOUR-10PK",
        "product_name": "Shred'ems Sour Cherry Punch 10mg×10",
        "category": ProductCategory.EDIBLE,
        "net_weight_g": Decimal("0.1"),  # 100mg total THC / expressed as grams
    },
    {
        "product_id": "GL-005",
        "sku": "TOPICAL-BALM-30ML",
        "product_name": "Dosecann Pain Relief Balm 30ml",
        "category": ProductCategory.TOPICAL,
        "net_weight_g": Decimal("30.0"),
    },
    {
        "product_id": "GL-006",
        "sku": "RAW-PAPERS-KS",
        "product_name": "RAW King Size Rolling Papers",
        "category": ProductCategory.ACCESSORY,
        "net_weight_g": Decimal("0"),
    },
]


# ── Mock Sale Webhook ───────────────────────────────────────────────────────

def mock_sale_webhook(
    location_id: str = "LOC-001",
    num_items: int = 2,
    event_type: EventType = EventType.SALE_CREATED,
) -> GreenlineSaleWebhook:
    """Generate a realistic sale.created webhook payload."""
    products = random.sample(
        [p for p in MOCK_PRODUCTS if p["category"] != ProductCategory.ACCESSORY],
        min(num_items, len(MOCK_PRODUCTS) - 1),
    )

    line_items = [
        GreenlineLineItem(
            product_id=p["product_id"],
            sku=p["sku"],
            product_name=p["product_name"],
            category=p["category"],
            net_weight_g=p["net_weight_g"],
            quantity=random.randint(1, 3),
            unit_price=Decimal(str(round(random.uniform(8.0, 65.0), 2))),
        )
        for p in products
    ]

    return GreenlineSaleWebhook(
        event_type=event_type,
        event_id=f"EVT-{uuid.uuid4().hex[:12]}",
        location_id=location_id,
        sale_id=f"SALE-{uuid.uuid4().hex[:8]}",
        timestamp=datetime.now(UTC),
        line_items=line_items,
    )


# ── Mock Inventory Snapshot ─────────────────────────────────────────────────

def mock_inventory_snapshot(
    location_id: str = "LOC-001",
    base_quantities: dict[str, Decimal] | None = None,
    drift_range: tuple[float, float] = (-2.0, 2.0),
) -> GreenlineInventorySnapshot:
    """
    Generate a mock closing inventory from Greenline.

    If base_quantities is provided, apply a small random "drift" to simulate
    real-world discrepancies (shrinkage, counting errors, etc.).
    """
    items = []
    for p in MOCK_PRODUCTS:
        if p["category"] == ProductCategory.ACCESSORY:
            continue

        if base_quantities and p["product_id"] in base_quantities:
            qty = base_quantities[p["product_id"]] + Decimal(
                str(round(random.uniform(*drift_range), 3))
            )
            qty = max(qty, Decimal("0"))
        else:
            qty = Decimal(str(round(random.uniform(10.0, 500.0), 3)))

        items.append(GreenlineInventoryItem(
            product_id=p["product_id"],
            sku=p["sku"],
            product_name=p["product_name"],
            category=p["category"],
            quantity_on_hand=qty,
        ))

    return GreenlineInventorySnapshot(
        location_id=location_id,
        snapshot_time=datetime.now(UTC),
        items=items,
    )


# ── Raw JSON (for testing parsers without Pydantic) ─────────────────────────

def mock_greenline_sale_json(location_id: str = "LOC-001") -> dict:
    """Return a raw dict matching Greenline's webhook JSON shape."""
    webhook = mock_sale_webhook(location_id)
    return webhook.model_dump(mode="json")


def mock_greenline_inventory_json(location_id: str = "LOC-001") -> dict:
    """Return a raw dict matching Greenline's /inventory/snapshots response."""
    snapshot = mock_inventory_snapshot(location_id)
    return {
        "location_id": snapshot.location_id,
        "snapshot_time": snapshot.snapshot_time.isoformat(),
        "items": [item.model_dump(mode="json") for item in snapshot.items],
    }
