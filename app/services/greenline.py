"""
VeriLeaf — Greenline POS Integration Service

Responsibilities:
  1. Fetch live inventory snapshots via REST API
  2. Ingest sale/adjustment webhooks (idempotent)
  3. Calculate gram-equivalency on every line item
"""
from __future__ import annotations

import uuid
import hmac
import hashlib
from decimal import Decimal
from datetime import datetime

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings, decrypt_token
from app.models.models import Location, RawPosEvent
from app.models.schemas import (
    GreenlineSaleWebhook,
    GreenlineInventorySnapshot,
    GreenlineInventoryItem,
    GreenlineLineItem,
    GRAM_EQUIVALENCY,
    ProductCategory,
)

logger = structlog.get_logger(__name__)


# ── HTTP Client ─────────────────────────────────────────────────────────────

class GreenlineClient:
    """Async HTTP wrapper for Greenline POS REST API."""

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or get_settings().greenline_base_url).rstrip("/")

    async def _get_token(self, session: AsyncSession, location_id: str) -> str:
        loc = await session.get(Location, location_id)
        if not loc:
            raise ValueError(f"Unknown location: {location_id}")
        return decrypt_token(loc.api_token_enc)

    async def fetch_inventory_snapshot(
        self, session: AsyncSession, location_id: str
    ) -> GreenlineInventorySnapshot:
        token = await self._get_token(session, location_id)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.base_url}/inventory/snapshots",
                params={"location_id": location_id},
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            data = resp.json()

        items = [
            GreenlineInventoryItem(
                product_id=item["product_id"],
                sku=item.get("sku", ""),
                product_name=item.get("product_name", ""),
                category=item.get("category", "dried"),
                quantity_on_hand=Decimal(str(item["quantity_on_hand"])),
            )
            for item in data.get("items", [])
        ]
        return GreenlineInventorySnapshot(
            location_id=location_id,
            snapshot_time=datetime.utcnow(),
            items=items,
        )

    async def fetch_sales(
        self, session: AsyncSession, location_id: str, since: datetime
    ) -> list[dict]:
        token = await self._get_token(session, location_id)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.base_url}/sales",
                params={"location_id": location_id, "since": since.isoformat()},
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json().get("sales", [])


# ── Gram Equivalency Calculator ─────────────────────────────────────────────

def calculate_gram_equivalency(item: GreenlineLineItem) -> Decimal:
    """
    Convert a line-item into Health Canada "dried gram equivalent".
    Formula: net_weight_g × quantity × equivalency_factor
    """
    factor = GRAM_EQUIVALENCY.get(item.category.value, Decimal("1.0"))
    return item.net_weight_g * item.quantity * factor


# ── Webhook Ingestor (idempotent) ───────────────────────────────────────────

def verify_webhook_signature(payload_bytes: bytes, signature: str) -> bool:
    """HMAC-SHA256 verification of Greenline webhook payloads."""
    secret = get_settings().greenline_webhook_secret.encode()
    expected = hmac.new(secret, payload_bytes, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def ingest_sale_webhook(
    session: AsyncSession, webhook: GreenlineSaleWebhook
) -> RawPosEvent | None:
    """
    Persist a sale webhook. Returns None if already processed (idempotent).

    Idempotency: INSERT … ON CONFLICT (location_id, external_event_id) DO NOTHING.
    This ensures processing the same Sale ID twice never double-counts inventory.
    """
    total_gram_eq = sum(
        calculate_gram_equivalency(item)
        for item in webhook.line_items
        if item.category != ProductCategory.ACCESSORY
    )

    stmt = (
        pg_insert(RawPosEvent)
        .values(
            id=uuid.uuid4(),
            location_id=webhook.location_id,
            external_event_id=webhook.event_id,
            event_type=webhook.event_type.value,
            payload=webhook.model_dump(mode="json"),
            gram_equivalency=total_gram_eq,
            received_at=datetime.utcnow(),
        )
        .on_conflict_do_nothing(constraint="uq_event_idempotent")
        .returning(RawPosEvent.id)
    )

    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    if row is None:
        logger.info("duplicate_webhook_skipped", event_id=webhook.event_id)
        return None

    await session.commit()
    logger.info(
        "webhook_ingested",
        event_id=webhook.event_id,
        gram_eq=str(total_gram_eq),
    )
    event = await session.get(RawPosEvent, row)
    return event
