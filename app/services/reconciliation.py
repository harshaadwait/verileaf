"""
VeriLeaf — Midnight Reconciliation Engine

Runs at 23:59 each night per location:
  1. Fetch Greenline's closing inventory via API
  2. Compute internal closing: opening + received − sold ± adjustments
  3. Compare. If |delta| > tolerance → create ComplianceDiscrepancy
  4. Freeze the DailyComplianceSnapshot as the "Golden Record"

Until ALL discrepancies for a date are acknowledged, reports cannot generate.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from datetime import date, datetime, timedelta
from collections import defaultdict

import structlog
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.models import (
    Location,
    RawPosEvent,
    DailyComplianceSnapshot,
    ComplianceDiscrepancy,
)
from app.models.schemas import (
    ReconciliationResult,
    ReconciliationSummary,
    GreenlineInventorySnapshot,
    EventType,
    ProductCategory,
    GRAM_EQUIVALENCY,
)
from app.services.greenline import GreenlineClient

logger = structlog.get_logger(__name__)


# ── Internal Balance Calculator ─────────────────────────────────────────────

async def _compute_internal_balances(
    session: AsyncSession, location_id: str, report_date: date
) -> dict[str, dict]:
    """
    Aggregate all raw events for the day into per-product mass-balance buckets.

    Returns: {product_id: {opening, received, sold, adjusted}}
    """
    # Get previous day's closing as today's opening
    prev_date = report_date - timedelta(days=1)
    prev_snapshots_q = select(DailyComplianceSnapshot).where(
        and_(
            DailyComplianceSnapshot.location_id == location_id,
            DailyComplianceSnapshot.report_date == prev_date,
        )
    )
    prev_result = await session.execute(prev_snapshots_q)
    prev_snapshots = {s.product_id: s.closing_qty for s in prev_result.scalars().all()}

    # Aggregate today's events from the immutable log
    day_start = datetime.combine(report_date, datetime.min.time())
    day_end = datetime.combine(report_date, datetime.max.time())

    events_q = select(RawPosEvent).where(
        and_(
            RawPosEvent.location_id == location_id,
            RawPosEvent.received_at >= day_start,
            RawPosEvent.received_at <= day_end,
        )
    )
    events_result = await session.execute(events_q)
    events = events_result.scalars().all()

    # Build per-product ledger
    ledger: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "opening": Decimal("0"),
            "received": Decimal("0"),
            "sold": Decimal("0"),
            "adjusted": Decimal("0"),
        }
    )

    # Seed openings from previous day
    for pid, closing in prev_snapshots.items():
        ledger[pid]["opening"] = closing

    # Process events
    for event in events:
        payload = event.payload
        line_items = payload.get("line_items", [])

        for item in line_items:
            pid = item["product_id"]
            cat = item.get("category", "dried")
            if cat == ProductCategory.ACCESSORY.value:
                continue

            factor = GRAM_EQUIVALENCY.get(cat, Decimal("1.0"))
            gram_qty = Decimal(str(item["net_weight_g"])) * int(item["quantity"]) * factor

            # Ensure product has an opening entry
            if pid not in ledger:
                ledger[pid]["opening"] = Decimal("0")

            if event.event_type == EventType.SALE_CREATED.value:
                ledger[pid]["sold"] += gram_qty
            elif event.event_type == EventType.SALE_VOIDED.value:
                ledger[pid]["sold"] -= gram_qty  # reversal
            elif event.event_type == EventType.INVENTORY_RECEIVED.value:
                ledger[pid]["received"] += gram_qty
            elif event.event_type == EventType.INVENTORY_ADJUSTMENT.value:
                ledger[pid]["adjusted"] += gram_qty  # can be negative

    return dict(ledger)


# ── Reconciliation Engine ───────────────────────────────────────────────────

async def run_reconciliation(
    session: AsyncSession,
    location_id: str,
    report_date: date,
    greenline_snapshot: GreenlineInventorySnapshot,
) -> ReconciliationSummary:
    """
    The heart of VeriLeaf.

    1. Compute internal closing per product
    2. Compare against Greenline's snapshot
    3. Persist DailyComplianceSnapshot rows
    4. Flag discrepancies
    """
    settings = get_settings()
    tolerance = Decimal(str(settings.reconciliation_tolerance_grams))

    # Step 1: Internal balances
    internal = await _compute_internal_balances(session, location_id, report_date)

    # Step 2: Greenline closing indexed by product
    greenline_closing: dict[str, Decimal] = {
        item.product_id: item.quantity_on_hand
        for item in greenline_snapshot.items
        if item.category != ProductCategory.ACCESSORY
    }

    # Merge product IDs from both sides
    all_products = set(internal.keys()) | set(greenline_closing.keys())

    results: list[ReconciliationResult] = []
    discrepancy_count = 0

    for pid in all_products:
        buckets = internal.get(pid, {
            "opening": Decimal("0"), "received": Decimal("0"),
            "sold": Decimal("0"), "adjusted": Decimal("0"),
        })

        opening = buckets["opening"]
        received = buckets["received"]
        sold = buckets["sold"]
        adjusted = buckets["adjusted"]
        internal_closing = opening + received - sold + adjusted

        gl_closing = greenline_closing.get(pid, Decimal("0"))
        delta = internal_closing - gl_closing
        within_tolerance = abs(delta) <= tolerance

        # Step 3: Persist golden-record snapshot
        snapshot = DailyComplianceSnapshot(
            id=uuid.uuid4(),
            location_id=location_id,
            report_date=report_date,
            product_id=pid,
            opening_qty=opening,
            qty_received=received,
            qty_sold=sold,
            qty_adjusted=adjusted,
            closing_qty=internal_closing,
            greenline_closing_qty=gl_closing,
            is_reconciled=within_tolerance,
            discrepancy_delta=delta,
        )
        session.add(snapshot)

        # Step 4: Flag discrepancies
        if not within_tolerance:
            discrepancy_count += 1
            disc = ComplianceDiscrepancy(
                id=uuid.uuid4(),
                snapshot_id=snapshot.id,
                location_id=location_id,
                report_date=report_date,
                product_id=pid,
                internal_qty=internal_closing,
                greenline_qty=gl_closing,
                delta=delta,
            )
            session.add(disc)
            logger.warning(
                "discrepancy_detected",
                product_id=pid,
                delta=str(delta),
                internal=str(internal_closing),
                greenline=str(gl_closing),
            )

        results.append(ReconciliationResult(
            product_id=pid,
            internal_closing=internal_closing,
            greenline_closing=gl_closing,
            delta=delta,
            is_within_tolerance=within_tolerance,
            report_date=report_date,
        ))

    await session.commit()

    summary = ReconciliationSummary(
        location_id=location_id,
        report_date=report_date,
        total_products=len(all_products),
        reconciled_count=len(all_products) - discrepancy_count,
        discrepancy_count=discrepancy_count,
        results=results,
    )

    logger.info(
        "reconciliation_complete",
        location_id=location_id,
        date=str(report_date),
        products=summary.total_products,
        discrepancies=summary.discrepancy_count,
    )

    return summary
