"""
VeriLeaf — Report Exporter

Generates government-ready CSV files:
  1. AGCO Monthly Retail Sales Report (Ontario)
  2. Health Canada CTLS (Cannabis Tracking and Licensing System) submission

CRITICAL RULE: Reports can ONLY be generated when ALL discrepancies for the
requested month are acknowledged. This is a compliance hard-stop.
"""
from __future__ import annotations

import csv
import io
from decimal import Decimal
from datetime import date
from calendar import monthrange

import structlog
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import (
    Location,
    DailyComplianceSnapshot,
    ComplianceDiscrepancy,
)

logger = structlog.get_logger(__name__)


class UnresolvedDiscrepancyError(Exception):
    """Raised when attempting to generate a report with unacknowledged discrepancies."""
    def __init__(self, count: int, location_id: str, month: int, year: int):
        self.count = count
        super().__init__(
            f"{count} unacknowledged discrepancies for location {location_id} "
            f"in {year}-{month:02d}. Reports blocked until resolved."
        )


# ── Guard: Discrepancy Check ───────────────────────────────────────────────

async def _assert_no_open_discrepancies(
    session: AsyncSession, location_id: str, year: int, month: int
) -> None:
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])

    q = select(func.count()).select_from(ComplianceDiscrepancy).where(
        and_(
            ComplianceDiscrepancy.location_id == location_id,
            ComplianceDiscrepancy.report_date >= first_day,
            ComplianceDiscrepancy.report_date <= last_day,
            ComplianceDiscrepancy.acknowledged == False,  # noqa: E712
        )
    )
    result = await session.execute(q)
    open_count = result.scalar_one()

    if open_count > 0:
        raise UnresolvedDiscrepancyError(open_count, location_id, month, year)


# ── Fetch Monthly Snapshots ─────────────────────────────────────────────────

async def _get_monthly_snapshots(
    session: AsyncSession, location_id: str, year: int, month: int
) -> list[DailyComplianceSnapshot]:
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])

    q = (
        select(DailyComplianceSnapshot)
        .where(
            and_(
                DailyComplianceSnapshot.location_id == location_id,
                DailyComplianceSnapshot.report_date >= first_day,
                DailyComplianceSnapshot.report_date <= last_day,
            )
        )
        .order_by(DailyComplianceSnapshot.report_date, DailyComplianceSnapshot.product_id)
    )
    result = await session.execute(q)
    return list(result.scalars().all())


# ── Report Exporter ─────────────────────────────────────────────────────────

class ReportExporter:
    """
    Generates compliance reports as in-memory CSV strings.
    Can be returned as HTTP StreamingResponse or saved to S3.
    """

    async def generate_agco_monthly(
        self, session: AsyncSession, location_id: str, year: int, month: int
    ) -> str:
        """
        Ontario AGCO Monthly Retail Sales Report.

        Layout:
          Store Name | Report Month | Product SKU | Product Name | Category |
          Opening Inventory (g) | Received (g) | Sold (g) | Adjustments (g) |
          Closing Inventory (g) | Reconciled (Y/N)
        """
        await _assert_no_open_discrepancies(session, location_id, year, month)
        snapshots = await _get_monthly_snapshots(session, location_id, year, month)

        location = await session.get(Location, location_id)
        store_name = location.store_name if location else "Unknown"

        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow([
            "Store Name", "Report Month", "Product SKU", "Product Name",
            "Category", "Opening Inventory (g)", "Received (g)", "Sold (g)",
            "Adjustments (g)", "Closing Inventory (g)", "Reconciled",
        ])

        # Aggregate to monthly totals per product
        monthly: dict[str, dict] = {}
        for snap in snapshots:
            if snap.product_id not in monthly:
                monthly[snap.product_id] = {
                    "sku": snap.sku,
                    "name": snap.product_name,
                    "category": snap.category,
                    "opening": snap.opening_qty,  # first day's opening
                    "received": Decimal("0"),
                    "sold": Decimal("0"),
                    "adjusted": Decimal("0"),
                    "closing": Decimal("0"),
                    "reconciled": True,
                }
            m = monthly[snap.product_id]
            m["received"] += snap.qty_received
            m["sold"] += snap.qty_sold
            m["adjusted"] += snap.qty_adjusted
            m["closing"] = snap.closing_qty  # last day's closing overwrites
            if not snap.is_reconciled:
                m["reconciled"] = False

        report_month_str = f"{year}-{month:02d}"
        for pid, m in sorted(monthly.items()):
            writer.writerow([
                store_name,
                report_month_str,
                m["sku"],
                m["name"],
                m["category"],
                f"{m['opening']:.3f}",
                f"{m['received']:.3f}",
                f"{m['sold']:.3f}",
                f"{m['adjusted']:.3f}",
                f"{m['closing']:.3f}",
                "Y" if m["reconciled"] else "N",
            ])

        csv_content = output.getvalue()
        logger.info("agco_report_generated", location=location_id, month=report_month_str)
        return csv_content

    async def generate_ctls_monthly(
        self, session: AsyncSession, location_id: str, year: int, month: int
    ) -> str:
        """
        Health Canada CTLS Monthly Submission.

        Key differences from AGCO:
          - All quantities in "dried gram equivalent"
          - Accessories excluded
          - Includes licence holder number
          - Layout per HC CTLS CSV specification

        Layout:
          Licence Number | Reporting Period | Product SKU | HC Category |
          Opening (g-eq) | Additions (g-eq) | Reductions (g-eq) |
          Adjustments (g-eq) | Closing (g-eq)
        """
        await _assert_no_open_discrepancies(session, location_id, year, month)
        snapshots = await _get_monthly_snapshots(session, location_id, year, month)

        location = await session.get(Location, location_id)

        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "Licence Number", "Reporting Period", "Product SKU", "HC Category",
            "Opening (g-eq)", "Additions (g-eq)", "Reductions (g-eq)",
            "Adjustments (g-eq)", "Closing (g-eq)",
        ])

        # Aggregate monthly, skip accessories (already gram-equivalent in snapshots)
        monthly: dict[str, dict] = {}
        for snap in snapshots:
            if snap.category == "accessory":
                continue
            if snap.product_id not in monthly:
                monthly[snap.product_id] = {
                    "sku": snap.sku,
                    "category": snap.category,
                    "opening": snap.opening_qty,
                    "additions": Decimal("0"),
                    "reductions": Decimal("0"),
                    "adjustments": Decimal("0"),
                    "closing": Decimal("0"),
                }
            m = monthly[snap.product_id]
            m["additions"] += snap.qty_received
            m["reductions"] += snap.qty_sold
            m["adjustments"] += snap.qty_adjusted
            m["closing"] = snap.closing_qty

        report_period = f"{year}-{month:02d}"
        licence_number = location.company_id if location else "PENDING"

        for pid, m in sorted(monthly.items()):
            writer.writerow([
                licence_number,
                report_period,
                m["sku"],
                m["category"],
                f"{m['opening']:.3f}",
                f"{m['additions']:.3f}",
                f"{m['reductions']:.3f}",
                f"{m['adjustments']:.3f}",
                f"{m['closing']:.3f}",
            ])

        csv_content = output.getvalue()
        logger.info("ctls_report_generated", location=location_id, month=report_period)
        return csv_content
