"""
VeriLeaf — Pydantic Schemas (strict validation)
Used for: webhook ingestion, API responses, report generation inputs.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator


# ── Enums ────────────────────────────────────────────────────────────────────

class Province(str, Enum):
    ON = "ON"
    AB = "AB"
    BC = "BC"
    QC = "QC"
    SK = "SK"
    MB = "MB"
    NB = "NB"
    NS = "NS"
    NL = "NL"
    PE = "PE"


class ProductCategory(str, Enum):
    DRIED = "dried"
    EXTRACT = "extract"
    EDIBLE = "edible"
    TOPICAL = "topical"
    ACCESSORY = "accessory"  # non-cannabis, excluded from CTLS


class EventType(str, Enum):
    SALE_CREATED = "sale.created"
    SALE_VOIDED = "sale.voided"
    INVENTORY_ADJUSTMENT = "inventory.adjustment"
    INVENTORY_RECEIVED = "inventory.received"


# ── Gram Equivalency Table (Health Canada standard) ─────────────────────────

GRAM_EQUIVALENCY: dict[str, Decimal] = {
    "dried":   Decimal("1.0"),
    "extract": Decimal("4.0"),     # 1g extract = 4g dried equivalent
    "edible":  Decimal("15.0"),    # 1g edible  = 15g dried equivalent
    "topical": Decimal("1.0"),     # varies; default 1:1
}


# ── Webhook / Ingestor Schemas ──────────────────────────────────────────────

class GreenlineLineItem(BaseModel):
    product_id: str
    sku: str = ""
    product_name: str = ""
    category: ProductCategory
    net_weight_g: Decimal = Field(ge=0, description="Net weight in grams")
    quantity: int = Field(ge=0)
    unit_price: Decimal = Field(ge=0)


class GreenlineSaleWebhook(BaseModel):
    """
    Canonical internal POS event. Greenline webhooks are validated directly
    into this type; other POS systems normalise into it before ingestion.
    """
    event_type: EventType
    event_id: str = Field(description="Idempotency key — unique per POS event")
    location_id: str
    sale_id: str
    timestamp: datetime
    line_items: list[GreenlineLineItem]


class GreenlineInventoryItem(BaseModel):
    product_id: str
    sku: str = ""
    product_name: str = ""
    category: ProductCategory
    quantity_on_hand: Decimal


class GreenlineInventorySnapshot(BaseModel):
    location_id: str
    snapshot_time: datetime
    items: list[GreenlineInventoryItem]


# ── Reconciliation Schemas ──────────────────────────────────────────────────

class ReconciliationResult(BaseModel):
    product_id: str
    internal_closing: Decimal
    greenline_closing: Decimal
    delta: Decimal
    is_within_tolerance: bool
    report_date: date


class ReconciliationSummary(BaseModel):
    location_id: str
    report_date: date
    total_products: int
    reconciled_count: int
    discrepancy_count: int
    results: list[ReconciliationResult]


# ── Discrepancy Acknowledgement ─────────────────────────────────────────────

class AcknowledgeDiscrepancy(BaseModel):
    acknowledged_by: str
    notes: str = ""


# ── Canonical POS-agnostic aliases ──────────────────────────────────────────
# Both Greenline and BLAZE normalise their snapshots to these types before
# the reconciliation engine sees them.

PosInventoryItem = GreenlineInventoryItem
PosInventorySnapshot = GreenlineInventorySnapshot


# ── BLAZE POS Schemas ────────────────────────────────────────────────────────

class BlazeEventType(str, Enum):
    SALE_CREATED        = "SALE_CREATED"
    SALE_VOIDED         = "SALE_VOIDED"
    INVENTORY_RECEIVED  = "INVENTORY_RECEIVED"
    INVENTORY_ADJUSTED  = "INVENTORY_ADJUSTED"


class BlazeLineItem(BaseModel):
    """A single line item as sent by BLAZE POS webhooks."""
    product_id: str
    sku: str = ""
    product_name: str = ""
    category: str                           # raw BLAZE string, e.g. "FLOWER"
    quantity: int = Field(ge=0)
    unit_weight_grams: Decimal = Field(ge=0, description="Weight of one unit in grams")
    unit_price: Decimal = Field(ge=0)


class BlazeTransactionData(BaseModel):
    transaction_id: str = ""               # empty for inventory events
    items: list[BlazeLineItem]


class BlazeSaleWebhook(BaseModel):
    """BLAZE webhook payload as received from the BLAZE platform."""
    event_type: BlazeEventType
    event_id: str = Field(description="Idempotency key from BLAZE")
    location_id: str
    occurred_at: datetime
    data: BlazeTransactionData


# ── Report Request ──────────────────────────────────────────────────────────

class ReportRequest(BaseModel):
    location_id: str
    month: int = Field(ge=1, le=12)
    year: int = Field(ge=2018)
    report_type: str = Field(pattern="^(agco|ctls)$")
