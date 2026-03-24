"""
VeriLeaf — FastAPI Application

Endpoints:
  POST /webhooks/greenline       — Ingest sale/adjustment events (idempotent)
  POST /reconcile/{location_id}  — Trigger reconciliation for a date
  GET  /reports/agco             — Generate AGCO monthly CSV
  GET  /reports/ctls             — Generate Health Canada CTLS CSV
  GET  /discrepancies            — List open discrepancies
  POST /discrepancies/{id}/ack   — Acknowledge a discrepancy
"""
from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import FastAPI, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_db
from app.models.models import ComplianceDiscrepancy
from app.models.schemas import (
    GreenlineSaleWebhook,
    AcknowledgeDiscrepancy,
    ReportRequest,
)
from app.services.greenline import ingest_sale_webhook, verify_webhook_signature, GreenlineClient
from app.services.reconciliation import run_reconciliation
from app.reports.exporter import ReportExporter, UnresolvedDiscrepancyError

import structlog

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="VeriLeaf",
    description="Automated Cannabis Compliance Engine",
    version="0.1.0",
)

DB = Annotated[AsyncSession, Depends(get_db)]


# ── Health ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "verileaf"}


# ── Webhook Ingestion ───────────────────────────────────────────────────────

@app.post("/webhooks/greenline", status_code=202)
async def receive_greenline_webhook(request: Request, db: DB):
    """
    Accept Greenline POS webhooks. HMAC-verified, idempotent.
    Returns 202 even for duplicates (safe retry).
    """
    body = await request.body()

    # Verify HMAC signature (skip in dev if no secret configured)
    signature = request.headers.get("X-Greenline-Signature", "")
    from app.core.config import get_settings
    if get_settings().greenline_webhook_secret and not verify_webhook_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = await request.json()
    webhook = GreenlineSaleWebhook(**payload)
    event = await ingest_sale_webhook(db, webhook)

    return {
        "accepted": True,
        "duplicate": event is None,
        "event_id": webhook.event_id,
    }


# ── Reconciliation Trigger ──────────────────────────────────────────────────

@app.post("/reconcile/{location_id}")
async def trigger_reconciliation(
    location_id: str, report_date: date, db: DB
):
    """
    Manually trigger reconciliation for a location + date.
    In production, this is called by Celery beat at 23:59.
    """
    client = GreenlineClient()
    snapshot = await client.fetch_inventory_snapshot(db, location_id)
    summary = await run_reconciliation(db, location_id, report_date, snapshot)

    return summary.model_dump(mode="json")


# ── Report Generation ───────────────────────────────────────────────────────

@app.get("/reports/agco")
async def download_agco_report(location_id: str, year: int, month: int, db: DB):
    exporter = ReportExporter()
    try:
        csv_content = await exporter.generate_agco_monthly(db, location_id, year, month)
    except UnresolvedDiscrepancyError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=AGCO_{location_id}_{year}-{month:02d}.csv"
        },
    )


@app.get("/reports/ctls")
async def download_ctls_report(location_id: str, year: int, month: int, db: DB):
    exporter = ReportExporter()
    try:
        csv_content = await exporter.generate_ctls_monthly(db, location_id, year, month)
    except UnresolvedDiscrepancyError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=CTLS_{location_id}_{year}-{month:02d}.csv"
        },
    )


# ── Discrepancy Management ──────────────────────────────────────────────────

@app.get("/discrepancies")
async def list_discrepancies(location_id: str, db: DB, acknowledged: bool = False):
    q = select(ComplianceDiscrepancy).where(
        and_(
            ComplianceDiscrepancy.location_id == location_id,
            ComplianceDiscrepancy.acknowledged == acknowledged,
        )
    ).order_by(ComplianceDiscrepancy.report_date.desc())

    result = await db.execute(q)
    discs = result.scalars().all()

    return [
        {
            "id": str(d.id),
            "report_date": d.report_date.isoformat(),
            "product_id": d.product_id,
            "internal_qty": str(d.internal_qty),
            "greenline_qty": str(d.greenline_qty),
            "delta": str(d.delta),
            "acknowledged": d.acknowledged,
            "notes": d.notes,
        }
        for d in discs
    ]


@app.post("/discrepancies/{discrepancy_id}/acknowledge")
async def acknowledge_discrepancy(
    discrepancy_id: str, body: AcknowledgeDiscrepancy, db: DB
):
    from datetime import datetime
    disc = await db.get(ComplianceDiscrepancy, discrepancy_id)
    if not disc:
        raise HTTPException(status_code=404, detail="Discrepancy not found")
    if disc.acknowledged:
        raise HTTPException(status_code=409, detail="Already acknowledged")

    disc.acknowledged = True
    disc.acknowledged_by = body.acknowledged_by
    disc.acknowledged_at = datetime.utcnow()
    disc.notes = body.notes
    await db.commit()

    return {"acknowledged": True, "id": discrepancy_id}
