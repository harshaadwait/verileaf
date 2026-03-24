"""
VeriLeaf — BLAZE POS Integration Service

Responsibilities:
  1. Map BLAZE product categories to VeriLeaf's canonical ProductCategory
  2. Normalise BLAZE webhooks into the internal GreenlineSaleWebhook format
     so the reconciliation engine is POS-agnostic
  3. Fetch live inventory snapshots from the BLAZE REST API
  4. Verify HMAC-SHA256 webhook signatures
  5. Ingest BLAZE events idempotently (delegates to the shared ingest path)

BLAZE API notes:
  - Auth:      Authorization: Bearer {api_key}
               Optional partner header: X-Blaze-Software-Id: {software_id}
  - Inventory: GET /inventory?location_id={id}
  - Sales:     GET /transactions?location_id={id}&startDate={iso}
  - Signature: X-Blaze-Signature (HMAC-SHA256 hex of raw request body)
"""
from __future__ import annotations

import hmac
import hashlib
from datetime import datetime, UTC
from decimal import Decimal

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings, decrypt_token
from app.models.models import Location, RawPosEvent
from app.models.schemas import (
    BlazeEventType,
    BlazeSaleWebhook,
    GreenlineInventoryItem,
    GreenlineInventorySnapshot,
    GreenlineLineItem,
    GreenlineSaleWebhook,
    EventType,
    ProductCategory,
)
from app.services.greenline import ingest_sale_webhook

logger = structlog.get_logger(__name__)


# ── Category map ─────────────────────────────────────────────────────────────

# BLAZE raw category string (upper-cased) → VeriLeaf ProductCategory
BLAZE_CATEGORY_MAP: dict[str, ProductCategory] = {
    # Dried flower
    "FLOWER":           ProductCategory.DRIED,
    "CANNABIS_FLOWER":  ProductCategory.DRIED,
    "PRE_ROLL":         ProductCategory.DRIED,
    "PRE-ROLL":         ProductCategory.DRIED,
    # Extracts / concentrates
    "CONCENTRATE":      ProductCategory.EXTRACT,
    "EXTRACT":          ProductCategory.EXTRACT,
    "OIL":              ProductCategory.EXTRACT,
    "TINCTURE":         ProductCategory.EXTRACT,
    "VAPE":             ProductCategory.EXTRACT,
    "CARTRIDGE":        ProductCategory.EXTRACT,
    "CANNABIS_EXTRACT": ProductCategory.EXTRACT,
    # Edibles / beverages
    "EDIBLE":           ProductCategory.EDIBLE,
    "BEVERAGE":         ProductCategory.EDIBLE,
    "CANNABIS_EDIBLE":  ProductCategory.EDIBLE,
    # Topicals
    "TOPICAL":          ProductCategory.TOPICAL,
    "CANNABIS_TOPICAL": ProductCategory.TOPICAL,
    # Non-cannabis accessories
    "ACCESSORY":        ProductCategory.ACCESSORY,
    "PARAPHERNALIA":    ProductCategory.ACCESSORY,
    "MERCHANDISE":      ProductCategory.ACCESSORY,
}

# BLAZE event type → internal EventType
_EVENT_TYPE_MAP: dict[BlazeEventType, EventType] = {
    BlazeEventType.SALE_CREATED:       EventType.SALE_CREATED,
    BlazeEventType.SALE_VOIDED:        EventType.SALE_VOIDED,
    BlazeEventType.INVENTORY_RECEIVED: EventType.INVENTORY_RECEIVED,
    BlazeEventType.INVENTORY_ADJUSTED: EventType.INVENTORY_ADJUSTMENT,
}


def normalize_blaze_category(raw: str) -> ProductCategory:
    """
    Map a BLAZE category string to VeriLeaf's ProductCategory.
    Unknown categories default to ACCESSORY (excluded from CTLS reports).
    """
    return BLAZE_CATEGORY_MAP.get(raw.upper().strip(), ProductCategory.ACCESSORY)


# ── Normalisation ─────────────────────────────────────────────────────────────

def normalize_blaze_webhook(webhook: BlazeSaleWebhook) -> GreenlineSaleWebhook:
    """
    Convert a BLAZE webhook into VeriLeaf's canonical internal event format.

    The normalised form is what gets stored in RawPosEvent.payload, so the
    reconciliation engine processes all POS events through the same code path
    regardless of their origin.
    """
    line_items = [
        GreenlineLineItem(
            product_id=item.product_id,
            sku=item.sku,
            product_name=item.product_name,
            category=normalize_blaze_category(item.category),
            net_weight_g=item.unit_weight_grams,
            quantity=item.quantity,
            unit_price=item.unit_price,
        )
        for item in webhook.data.items
    ]
    return GreenlineSaleWebhook(
        event_type=_EVENT_TYPE_MAP[webhook.event_type],
        event_id=webhook.event_id,
        location_id=webhook.location_id,
        sale_id=webhook.data.transaction_id or webhook.event_id,
        timestamp=webhook.occurred_at,
        line_items=line_items,
    )


# ── HTTP Client ───────────────────────────────────────────────────────────────

class BlazeClient:
    """Async HTTP wrapper for the BLAZE POS REST API."""

    def __init__(self, base_url: str | None = None):
        settings = get_settings()
        self.base_url = (base_url or settings.blaze_base_url).rstrip("/")
        self._software_id = settings.blaze_software_id

    def _auth_headers(self, token: str) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {token}"}
        if self._software_id:
            headers["X-Blaze-Software-Id"] = self._software_id
        return headers

    async def _get_token(self, session: AsyncSession, location_id: str) -> str:
        loc = await session.get(Location, location_id)
        if not loc:
            raise ValueError(f"Unknown location: {location_id}")
        return decrypt_token(loc.api_token_enc)

    async def fetch_inventory_snapshot(
        self, session: AsyncSession, location_id: str
    ) -> GreenlineInventorySnapshot:
        """
        Fetch current on-hand quantities from BLAZE and return them as a
        PosInventorySnapshot (GreenlineInventorySnapshot alias).
        Accessories are excluded — they carry no gram-equivalency.
        """
        token = await self._get_token(session, location_id)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.base_url}/inventory",
                params={"location_id": location_id},
                headers=self._auth_headers(token),
            )
            resp.raise_for_status()
            data = resp.json()

        items = []
        for raw in data.get("inventory", []):
            category = normalize_blaze_category(raw.get("category", ""))
            if category == ProductCategory.ACCESSORY:
                continue
            items.append(GreenlineInventoryItem(
                product_id=raw["product_id"],
                sku=raw.get("sku", ""),
                product_name=raw.get("product_name", ""),
                category=category,
                quantity_on_hand=Decimal(str(raw["quantity_on_hand"])),
            ))

        return GreenlineInventorySnapshot(
            location_id=location_id,
            snapshot_time=datetime.now(UTC),
            items=items,
        )

    async def fetch_sales(
        self, session: AsyncSession, location_id: str, since: datetime
    ) -> list[dict]:
        token = await self._get_token(session, location_id)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.base_url}/transactions",
                params={"location_id": location_id, "startDate": since.isoformat()},
                headers=self._auth_headers(token),
            )
            resp.raise_for_status()
            return resp.json().get("transactions", [])


# ── Webhook verification ──────────────────────────────────────────────────────

def verify_blaze_webhook(payload_bytes: bytes, signature: str) -> bool:
    """HMAC-SHA256 verification of BLAZE webhook payloads."""
    secret = get_settings().blaze_webhook_secret.encode()
    expected = hmac.new(secret, payload_bytes, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


# ── Webhook ingestion (idempotent) ────────────────────────────────────────────

async def ingest_blaze_webhook(
    session: AsyncSession, webhook: BlazeSaleWebhook
) -> RawPosEvent | None:
    """
    Normalise a BLAZE webhook to the canonical internal format and persist it.

    Delegates to ingest_sale_webhook so idempotency logic, gram-equivalency
    calculation, and the immutable audit log are shared with the Greenline path.
    Returns None if the event was already processed (duplicate).
    """
    canonical = normalize_blaze_webhook(webhook)
    logger.info(
        "blaze_webhook_normalised",
        event_id=webhook.event_id,
        event_type=webhook.event_type,
        canonical_type=canonical.event_type,
        items=len(canonical.line_items),
    )
    return await ingest_sale_webhook(session, canonical)
