"""
VeriLeaf — FastAPI Integration Tests

Exercises every endpoint with a real SQLite DB and httpx AsyncClient.
External dependencies (GreenlineClient, pg_insert) are mocked at the
call-site so tests run without network access or PostgreSQL.

Test classes:
  TestHealth              — GET /health
  TestWebhookIngestion    — POST /webhooks/greenline
  TestReconciliation      — POST /reconcile/{location_id}
  TestDiscrepancies       — GET/POST /discrepancies
  TestReports             — GET /reports/agco and /reports/ctls
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy import select

from app.models.models import DailyComplianceSnapshot, ComplianceDiscrepancy
from app.services.mock_greenline import mock_greenline_sale_json, mock_inventory_snapshot


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_snapshot(location_id: str, product_id: str = "GL-001", reconciled: bool = True,
                   report_date: date = date(2025, 1, 15)) -> DailyComplianceSnapshot:
    return DailyComplianceSnapshot(
        id=uuid.uuid4(),
        location_id=location_id,
        report_date=report_date,
        product_id=product_id,
        sku="WAPPA-3.5",
        product_name="Redecan Wappa 3.5g",
        category="dried",
        opening_qty=Decimal("100.000"),
        qty_received=Decimal("50.000"),
        qty_sold=Decimal("30.000"),
        qty_adjusted=Decimal("0.000"),
        closing_qty=Decimal("120.000"),
        greenline_closing_qty=Decimal("120.000") if reconciled else Decimal("110.000"),
        is_reconciled=reconciled,
        discrepancy_delta=Decimal("0.000") if reconciled else Decimal("10.000"),
    )


def _make_discrepancy(snapshot: DailyComplianceSnapshot) -> ComplianceDiscrepancy:
    return ComplianceDiscrepancy(
        id=uuid.uuid4(),
        snapshot_id=snapshot.id,
        location_id=snapshot.location_id,
        report_date=snapshot.report_date,
        product_id=snapshot.product_id,
        internal_qty=Decimal("120.000"),
        greenline_qty=Decimal("110.000"),
        delta=Decimal("10.000"),
    )


async def _seed(engine, *rows):
    """Insert ORM rows into the test DB and commit."""
    Factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Factory() as db:
        for row in rows:
            db.add(row)
        await db.commit()


# ── Health ───────────────────────────────────────────────────────────────────

class TestHealth:
    async def test_returns_ok(self, ac):
        resp = await ac.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok", "service": "verileaf"}


# ── Webhook Ingestion ────────────────────────────────────────────────────────

class TestWebhookIngestion:
    async def test_valid_webhook_accepted(self, ac, location):
        payload = mock_greenline_sale_json()
        payload["location_id"] = location

        mock_event = MagicMock()
        mock_event.id = uuid.uuid4()

        with patch("app.api.main.ingest_sale_webhook", new=AsyncMock(return_value=mock_event)):
            resp = await ac.post("/webhooks/greenline", json=payload)

        assert resp.status_code == 202
        data = resp.json()
        assert data["accepted"] is True
        assert data["duplicate"] is False
        assert data["event_id"] == payload["event_id"]

    async def test_duplicate_webhook_flagged(self, ac, location):
        payload = mock_greenline_sale_json()
        payload["location_id"] = location

        with patch("app.api.main.ingest_sale_webhook", new=AsyncMock(return_value=None)):
            resp = await ac.post("/webhooks/greenline", json=payload)

        assert resp.status_code == 202
        data = resp.json()
        assert data["accepted"] is True
        assert data["duplicate"] is True

    async def test_malformed_payload_returns_422(self, ac):
        resp = await ac.post("/webhooks/greenline", json={"bad": "payload"})
        assert resp.status_code == 422

    async def test_invalid_signature_returns_401(self, ac, location, monkeypatch):
        from app.core.config import get_settings
        monkeypatch.setenv("VERILEAF_GREENLINE_WEBHOOK_SECRET", "test_secret")
        get_settings.cache_clear()
        try:
            payload = mock_greenline_sale_json()
            payload["location_id"] = location
            resp = await ac.post(
                "/webhooks/greenline",
                json=payload,
                headers={"X-Greenline-Signature": "bad_signature"},
            )
            assert resp.status_code == 401
        finally:
            monkeypatch.delenv("VERILEAF_GREENLINE_WEBHOOK_SECRET", raising=False)
            get_settings.cache_clear()


# ── Reconciliation ───────────────────────────────────────────────────────────

class TestReconciliation:
    async def test_returns_summary_shape(self, ac, location):
        snapshot = mock_inventory_snapshot(location_id=location)

        with patch("app.api.main.GreenlineClient") as MockClient:
            MockClient.return_value.fetch_inventory_snapshot = AsyncMock(return_value=snapshot)
            resp = await ac.post(
                f"/reconcile/{location}",
                params={"report_date": "2025-01-15"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["location_id"] == location
        assert data["report_date"] == "2025-01-15"
        assert isinstance(data["total_products"], int)
        assert isinstance(data["discrepancy_count"], int)
        assert isinstance(data["results"], list)

    async def test_persists_snapshot_rows(self, engine, ac, location):
        snapshot = mock_inventory_snapshot(location_id=location)
        expected_products = len(snapshot.items)

        with patch("app.api.main.GreenlineClient") as MockClient:
            MockClient.return_value.fetch_inventory_snapshot = AsyncMock(return_value=snapshot)
            resp = await ac.post(
                f"/reconcile/{location}",
                params={"report_date": "2025-01-15"},
            )

        assert resp.status_code == 200
        assert resp.json()["total_products"] == expected_products

        Factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with Factory() as db:
            result = await db.execute(
                select(DailyComplianceSnapshot).where(
                    DailyComplianceSnapshot.location_id == location
                )
            )
            rows = result.scalars().all()
        assert len(rows) == expected_products

    async def test_large_drift_creates_discrepancy(self, engine, ac, location):
        """Snapshot with obvious drift (no prior events) creates discrepancy rows."""
        snapshot = mock_inventory_snapshot(location_id=location)

        with patch("app.api.main.GreenlineClient") as MockClient:
            MockClient.return_value.fetch_inventory_snapshot = AsyncMock(return_value=snapshot)
            resp = await ac.post(
                f"/reconcile/{location}",
                params={"report_date": "2025-01-15"},
            )

        assert resp.status_code == 200
        data = resp.json()
        # No events ingested → internal closing = 0 for all products.
        # Greenline shows non-zero quantities → every product is a discrepancy.
        assert data["discrepancy_count"] > 0

        Factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with Factory() as db:
            result = await db.execute(
                select(ComplianceDiscrepancy).where(
                    ComplianceDiscrepancy.location_id == location
                )
            )
            discs = result.scalars().all()
        assert len(discs) == data["discrepancy_count"]


# ── Discrepancies ────────────────────────────────────────────────────────────

class TestDiscrepancies:
    async def test_empty_list(self, ac):
        resp = await ac.get("/discrepancies", params={"location_id": "LOC-001"})
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_lists_open_discrepancies(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)

        resp = await ac.get("/discrepancies", params={"location_id": location})
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 1
        item = items[0]
        assert item["product_id"] == snap.product_id
        assert item["acknowledged"] is False
        assert item["delta"] == "10.000"

    async def test_acknowledge_sets_flag(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)
        disc_id = str(disc.id)

        resp = await ac.post(
            f"/discrepancies/{disc_id}/acknowledge",
            json={"acknowledged_by": "manager@example.com", "notes": "Verified count"},
        )
        assert resp.status_code == 200
        assert resp.json()["acknowledged"] is True

    async def test_acknowledged_hidden_from_default_list(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)
        disc_id = str(disc.id)

        await ac.post(
            f"/discrepancies/{disc_id}/acknowledge",
            json={"acknowledged_by": "manager@example.com"},
        )
        resp = await ac.get("/discrepancies", params={"location_id": location})
        assert resp.json() == []

    async def test_acknowledge_unknown_returns_404(self, ac):
        resp = await ac.post(
            f"/discrepancies/{uuid.uuid4()}/acknowledge",
            json={"acknowledged_by": "manager@example.com"},
        )
        assert resp.status_code == 404

    async def test_double_acknowledge_returns_409(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)
        disc_id = str(disc.id)

        await ac.post(
            f"/discrepancies/{disc_id}/acknowledge",
            json={"acknowledged_by": "manager@example.com"},
        )
        resp = await ac.post(
            f"/discrepancies/{disc_id}/acknowledge",
            json={"acknowledged_by": "manager@example.com"},
        )
        assert resp.status_code == 409


# ── Reports ──────────────────────────────────────────────────────────────────

class TestReports:
    async def test_agco_empty_when_no_snapshots(self, ac, location):
        """No snapshots + no discrepancies → 200 with header row only."""
        resp = await ac.get(
            "/reports/agco",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        lines = [l for l in resp.text.strip().split("\n") if l]
        assert len(lines) == 1  # header only

    async def test_agco_blocked_by_open_discrepancy(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)

        resp = await ac.get(
            "/reports/agco",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 409
        assert "unacknowledged" in resp.json()["detail"]

    async def test_agco_generates_csv_rows(self, engine, ac, location):
        await _seed(engine, _make_snapshot(location))

        resp = await ac.get(
            "/reports/agco",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert resp.headers["content-disposition"] == "attachment; filename=AGCO_LOC-001_2025-01.csv"
        lines = [l for l in resp.text.strip().split("\n") if l]
        assert len(lines) == 2  # header + 1 product
        assert "Redecan Wappa" in resp.text
        assert "100.000" in resp.text  # opening qty

    async def test_agco_unblocked_after_acknowledge(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)

        # Blocked initially
        resp = await ac.get(
            "/reports/agco",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 409

        # Acknowledge the discrepancy
        await ac.post(
            f"/discrepancies/{disc.id}/acknowledge",
            json={"acknowledged_by": "manager@example.com", "notes": "Investigated"},
        )

        # Now unblocked
        resp = await ac.get(
            "/reports/agco",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 200

    async def test_ctls_blocked_by_open_discrepancy(self, engine, ac, location):
        snap = _make_snapshot(location, reconciled=False)
        disc = _make_discrepancy(snap)
        await _seed(engine, snap, disc)

        resp = await ac.get(
            "/reports/ctls",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 409

    async def test_ctls_excludes_accessories(self, engine, ac, location):
        accessory_snap = DailyComplianceSnapshot(
            id=uuid.uuid4(),
            location_id=location,
            report_date=date(2025, 1, 15),
            product_id="GL-ACC-001",
            sku="GRINDER-001",
            product_name="Metal Grinder",
            category="accessory",
            opening_qty=Decimal("10.000"),
            qty_received=Decimal("5.000"),
            qty_sold=Decimal("2.000"),
            qty_adjusted=Decimal("0.000"),
            closing_qty=Decimal("13.000"),
            greenline_closing_qty=Decimal("13.000"),
            is_reconciled=True,
            discrepancy_delta=Decimal("0.000"),
        )
        await _seed(engine, accessory_snap)

        resp = await ac.get(
            "/reports/ctls",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 200
        assert "Metal Grinder" not in resp.text
        assert "GRINDER" not in resp.text

    async def test_ctls_includes_dried_products(self, engine, ac, location):
        await _seed(engine, _make_snapshot(location))

        resp = await ac.get(
            "/reports/ctls",
            params={"location_id": location, "year": 2025, "month": 1},
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        lines = [l for l in resp.text.strip().split("\n") if l]
        assert len(lines) == 2  # header + 1 product
        assert "CO-TEST-001" in resp.text  # licence number = company_id
