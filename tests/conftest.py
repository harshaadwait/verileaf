"""
Shared fixtures for VeriLeaf integration tests.

Uses SQLite in-memory (aiosqlite + StaticPool) so tests run without a
running PostgreSQL instance. StaticPool ensures all sessions share the
same connection, which is required for in-memory SQLite to be visible
across sessions within a single test.
"""
from __future__ import annotations

import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool

from app.api.main import app
from app.core.config import get_db
from app.models.models import Base, Location

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def engine():
    """Fresh in-memory SQLite DB per test."""
    eng = create_async_engine(
        TEST_DB_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def ac(engine):
    """AsyncClient wired to the test DB via dependency override."""
    Factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with Factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def location(engine):
    """Seed a Greenline Location row and return its ID."""
    Factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Factory() as db:
        loc = Location(
            id="LOC-001",
            company_id="CO-TEST-001",
            store_name="Test Dispensary",
            province="ON",
            api_token_enc="fake_encrypted_token",
            pos_system="greenline",
        )
        db.add(loc)
        await db.commit()
    return "LOC-001"


@pytest_asyncio.fixture
async def blaze_location(engine):
    """Seed a BLAZE Location row and return its ID."""
    Factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Factory() as db:
        loc = Location(
            id="BLZ-001",
            company_id="CO-TEST-001",
            store_name="Test Dispensary (BLAZE)",
            province="ON",
            api_token_enc="fake_encrypted_token",
            pos_system="blaze",
        )
        db.add(loc)
        await db.commit()
    return "BLZ-001"
