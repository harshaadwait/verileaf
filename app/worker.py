"""
VeriLeaf — Celery Worker

Scheduled task: Midnight Reconciliation
Runs at 23:59 for each active location.
"""
from __future__ import annotations

import asyncio
from datetime import date

from celery import Celery
from celery.schedules import crontab

from app.core.config import get_settings, AsyncSessionLocal
from app.services.greenline import GreenlineClient
from app.services.blaze import BlazeClient
from app.services.reconciliation import run_reconciliation

settings = get_settings()

celery_app = Celery("verileaf", broker=settings.celery_broker_url)

celery_app.conf.beat_schedule = {
    "midnight-reconciliation": {
        "task": "app.worker.reconcile_all_locations",
        "schedule": crontab(
            hour=settings.midnight_cron_hour,
            minute=settings.midnight_cron_minute,
        ),
    },
}
celery_app.conf.timezone = "America/Toronto"


@celery_app.task(name="app.worker.reconcile_all_locations")
def reconcile_all_locations():
    """Fan-out: fetch all active locations and reconcile each."""
    asyncio.run(_reconcile_all())


async def _reconcile_all():
    from sqlalchemy import select
    from app.models.models import Location

    async with AsyncSessionLocal() as session:
        q = select(Location).where(Location.is_active == True)  # noqa: E712
        result = await session.execute(q)
        locations = result.scalars().all()

    for loc in locations:
        reconcile_single_location.delay(loc.id)


@celery_app.task(name="app.worker.reconcile_single_location", bind=True, max_retries=3)
def reconcile_single_location(self, location_id: str):
    """Reconcile one location. Retries up to 3× on API failures."""
    try:
        asyncio.run(_reconcile_one(location_id))
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


async def _reconcile_one(location_id: str):
    from app.models.models import Location
    async with AsyncSessionLocal() as session:
        loc = await session.get(Location, location_id)
        client = BlazeClient() if (loc and loc.pos_system == "blaze") else GreenlineClient()
        snapshot = await client.fetch_inventory_snapshot(session, location_id)
        await run_reconciliation(session, location_id, date.today(), snapshot)
