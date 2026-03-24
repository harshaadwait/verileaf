"""
VeriLeaf — BLAZE POS Integration Tests

Test classes:
  TestBlazeCategories       — BLAZE category string → ProductCategory mapping
  TestBlazeNormalisation    — BlazeSaleWebhook → canonical GreenlineSaleWebhook
  TestBlazeWebhookEndpoint  — POST /webhooks/blaze integration tests
  TestBlazeReconcileRouting — POST /reconcile/{id} routes to BlazeClient for blaze locations
"""
from __future__ import annotations

import uuid
from datetime import datetime, UTC
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.schemas import (
    BlazeEventType,
    BlazeLineItem,
    BlazeSaleWebhook,
    BlazeTransactionData,
    EventType,
    ProductCategory,
)
from app.services.blaze import (
    BLAZE_CATEGORY_MAP,
    normalize_blaze_category,
    normalize_blaze_webhook,
)
from app.services.mock_greenline import mock_inventory_snapshot


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_blaze_webhook(
    event_type: BlazeEventType = BlazeEventType.SALE_CREATED,
    location_id: str = "BLZ-001",
    n_items: int = 1,
) -> BlazeSaleWebhook:
    items = [
        BlazeLineItem(
            product_id=f"BLZP-{i:03d}",
            sku=f"WPP-3.5-{i}",
            product_name=f"Wappa 3.5g (unit {i})",
            category="FLOWER",
            quantity=2,
            unit_weight_grams=Decimal("3.5"),
            unit_price=Decimal("25.00"),
        )
        for i in range(n_items)
    ]
    return BlazeSaleWebhook(
        event_type=event_type,
        event_id=f"evt-blz-{uuid.uuid4().hex[:12]}",
        location_id=location_id,
        occurred_at=datetime.now(UTC),
        data=BlazeTransactionData(transaction_id=f"txn-{uuid.uuid4().hex[:8]}", items=items),
    )


def _webhook_payload(webhook: BlazeSaleWebhook) -> dict:
    return webhook.model_dump(mode="json")


# ── Category Mapping ──────────────────────────────────────────────────────────

class TestBlazeCategories:
    def test_flower_maps_to_dried(self):
        assert normalize_blaze_category("FLOWER") == ProductCategory.DRIED

    def test_pre_roll_maps_to_dried(self):
        assert normalize_blaze_category("PRE_ROLL") == ProductCategory.DRIED

    def test_pre_roll_hyphen_maps_to_dried(self):
        assert normalize_blaze_category("PRE-ROLL") == ProductCategory.DRIED

    def test_concentrate_maps_to_extract(self):
        assert normalize_blaze_category("CONCENTRATE") == ProductCategory.EXTRACT

    def test_vape_maps_to_extract(self):
        assert normalize_blaze_category("VAPE") == ProductCategory.EXTRACT

    def test_cartridge_maps_to_extract(self):
        assert normalize_blaze_category("CARTRIDGE") == ProductCategory.EXTRACT

    def test_oil_maps_to_extract(self):
        assert normalize_blaze_category("OIL") == ProductCategory.EXTRACT

    def test_edible_maps_to_edible(self):
        assert normalize_blaze_category("EDIBLE") == ProductCategory.EDIBLE

    def test_beverage_maps_to_edible(self):
        assert normalize_blaze_category("BEVERAGE") == ProductCategory.EDIBLE

    def test_topical_maps_to_topical(self):
        assert normalize_blaze_category("TOPICAL") == ProductCategory.TOPICAL

    def test_accessory_maps_to_accessory(self):
        assert normalize_blaze_category("ACCESSORY") == ProductCategory.ACCESSORY

    def test_paraphernalia_maps_to_accessory(self):
        assert normalize_blaze_category("PARAPHERNALIA") == ProductCategory.ACCESSORY

    def test_unknown_category_defaults_to_accessory(self):
        assert normalize_blaze_category("MYSTERY_PRODUCT") == ProductCategory.ACCESSORY

    def test_lowercase_input_normalised(self):
        assert normalize_blaze_category("flower") == ProductCategory.DRIED

    def test_mixed_case_input_normalised(self):
        assert normalize_blaze_category("Concentrate") == ProductCategory.EXTRACT

    def test_all_map_entries_produce_valid_category(self):
        for raw, expected in BLAZE_CATEGORY_MAP.items():
            assert normalize_blaze_category(raw) == expected


# ── Webhook Normalisation ─────────────────────────────────────────────────────

class TestBlazeNormalisation:
    def test_sale_created_maps_to_canonical_event_type(self):
        webhook = _make_blaze_webhook(BlazeEventType.SALE_CREATED)
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.event_type == EventType.SALE_CREATED

    def test_sale_voided_maps_to_canonical_event_type(self):
        webhook = _make_blaze_webhook(BlazeEventType.SALE_VOIDED)
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.event_type == EventType.SALE_VOIDED

    def test_inventory_received_maps_correctly(self):
        webhook = _make_blaze_webhook(BlazeEventType.INVENTORY_RECEIVED)
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.event_type == EventType.INVENTORY_RECEIVED

    def test_inventory_adjusted_maps_correctly(self):
        webhook = _make_blaze_webhook(BlazeEventType.INVENTORY_ADJUSTED)
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.event_type == EventType.INVENTORY_ADJUSTMENT

    def test_event_id_preserved(self):
        webhook = _make_blaze_webhook()
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.event_id == webhook.event_id

    def test_location_id_preserved(self):
        webhook = _make_blaze_webhook(location_id="BLZ-STORE-42")
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.location_id == "BLZ-STORE-42"

    def test_line_items_count_preserved(self):
        webhook = _make_blaze_webhook(n_items=3)
        canonical = normalize_blaze_webhook(webhook)
        assert len(canonical.line_items) == 3

    def test_unit_weight_grams_becomes_net_weight_g(self):
        webhook = _make_blaze_webhook()
        item = webhook.data.items[0]
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.line_items[0].net_weight_g == item.unit_weight_grams

    def test_flower_category_normalised_in_line_items(self):
        webhook = _make_blaze_webhook()
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.line_items[0].category == ProductCategory.DRIED

    def test_transaction_id_becomes_sale_id(self):
        webhook = _make_blaze_webhook()
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.sale_id == webhook.data.transaction_id

    def test_empty_transaction_id_falls_back_to_event_id(self):
        webhook = _make_blaze_webhook()
        webhook.data.transaction_id = ""
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.sale_id == webhook.event_id

    def test_occurred_at_becomes_timestamp(self):
        webhook = _make_blaze_webhook()
        canonical = normalize_blaze_webhook(webhook)
        assert canonical.timestamp == webhook.occurred_at

    def test_product_metadata_preserved(self):
        webhook = _make_blaze_webhook()
        blaze_item = webhook.data.items[0]
        canonical_item = normalize_blaze_webhook(webhook).line_items[0]
        assert canonical_item.product_id == blaze_item.product_id
        assert canonical_item.sku == blaze_item.sku
        assert canonical_item.product_name == blaze_item.product_name
        assert canonical_item.unit_price == blaze_item.unit_price


# ── POST /webhooks/blaze ──────────────────────────────────────────────────────

class TestBlazeWebhookEndpoint:
    async def test_valid_webhook_accepted(self, ac, blaze_location):
        payload = _webhook_payload(_make_blaze_webhook(location_id=blaze_location))
        mock_event = MagicMock()
        mock_event.id = uuid.uuid4()

        with patch("app.api.main.ingest_blaze_webhook", new=AsyncMock(return_value=mock_event)):
            resp = await ac.post("/webhooks/blaze", json=payload)

        assert resp.status_code == 202
        data = resp.json()
        assert data["accepted"] is True
        assert data["duplicate"] is False
        assert data["event_id"] == payload["event_id"]

    async def test_duplicate_webhook_flagged(self, ac, blaze_location):
        payload = _webhook_payload(_make_blaze_webhook(location_id=blaze_location))

        with patch("app.api.main.ingest_blaze_webhook", new=AsyncMock(return_value=None)):
            resp = await ac.post("/webhooks/blaze", json=payload)

        assert resp.status_code == 202
        assert resp.json()["duplicate"] is True

    async def test_all_blaze_event_types_accepted(self, ac, blaze_location):
        for event_type in BlazeEventType:
            payload = _webhook_payload(_make_blaze_webhook(
                event_type=event_type, location_id=blaze_location
            ))
            mock_event = MagicMock()
            mock_event.id = uuid.uuid4()
            with patch("app.api.main.ingest_blaze_webhook", new=AsyncMock(return_value=mock_event)):
                resp = await ac.post("/webhooks/blaze", json=payload)
            assert resp.status_code == 202, f"Failed for event_type={event_type}"

    async def test_malformed_payload_returns_422(self, ac):
        resp = await ac.post("/webhooks/blaze", json={"bad": "payload"})
        assert resp.status_code == 422

    async def test_invalid_signature_returns_401(self, ac, blaze_location, monkeypatch):
        from app.core.config import get_settings
        monkeypatch.setenv("VERILEAF_BLAZE_WEBHOOK_SECRET", "test_blaze_secret")
        get_settings.cache_clear()
        try:
            payload = _webhook_payload(_make_blaze_webhook(location_id=blaze_location))
            resp = await ac.post(
                "/webhooks/blaze",
                json=payload,
                headers={"X-Blaze-Signature": "wrong_signature"},
            )
            assert resp.status_code == 401
        finally:
            monkeypatch.delenv("VERILEAF_BLAZE_WEBHOOK_SECRET", raising=False)
            get_settings.cache_clear()

    async def test_multi_item_webhook_accepted(self, ac, blaze_location):
        payload = _webhook_payload(_make_blaze_webhook(location_id=blaze_location, n_items=4))
        mock_event = MagicMock()
        mock_event.id = uuid.uuid4()

        with patch("app.api.main.ingest_blaze_webhook", new=AsyncMock(return_value=mock_event)):
            resp = await ac.post("/webhooks/blaze", json=payload)

        assert resp.status_code == 202


# ── Reconcile routing ─────────────────────────────────────────────────────────

class TestBlazeReconcileRouting:
    async def test_blaze_location_uses_blaze_client(self, ac, blaze_location):
        snapshot = mock_inventory_snapshot(location_id=blaze_location)

        with patch("app.api.main.BlazeClient") as MockBlaze, \
             patch("app.api.main.GreenlineClient") as MockGreenline:
            MockBlaze.return_value.fetch_inventory_snapshot = AsyncMock(return_value=snapshot)
            resp = await ac.post(
                f"/reconcile/{blaze_location}",
                params={"report_date": "2025-01-15"},
            )

        assert resp.status_code == 200
        MockBlaze.assert_called_once()
        MockGreenline.assert_not_called()

    async def test_greenline_location_uses_greenline_client(self, ac, location):
        snapshot = mock_inventory_snapshot(location_id=location)

        with patch("app.api.main.GreenlineClient") as MockGreenline, \
             patch("app.api.main.BlazeClient") as MockBlaze:
            MockGreenline.return_value.fetch_inventory_snapshot = AsyncMock(return_value=snapshot)
            resp = await ac.post(
                f"/reconcile/{location}",
                params={"report_date": "2025-01-15"},
            )

        assert resp.status_code == 200
        MockGreenline.assert_called_once()
        MockBlaze.assert_not_called()

    async def test_unknown_location_returns_404(self, ac):
        resp = await ac.post(
            "/reconcile/NONEXISTENT",
            params={"report_date": "2025-01-15"},
        )
        assert resp.status_code == 404
