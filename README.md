# VeriLeaf — Automated Cannabis Compliance Engine

**SaaS middleware** that automates provincial (AGCO/SQDC/AGLC) and federal (Health Canada CTLS) reporting for cannabis retailers across Canada.

## Architecture

```
┌──────────────┐    Webhooks     ┌─────────────────────┐
│  Greenline   │ ──────────────► │  FastAPI Ingestor    │
│  BLAZE POS   │                 │  (HMAC verified,     │
└──────────────┘                 │   idempotent)        │
                                 └─────────┬───────────┘
                                           │
                    ┌──────────────────────┐│┌──────────────────────┐
                    │  Track 1:            │││  Track 2:            │
                    │  RawPosEvent         │││  DailyCompliance     │
                    │  (immutable log)     │││  Snapshot            │
                    └──────────────────────┘│└──────────────────────┘
                                           │
                                 ┌─────────▼───────────┐
                                 │  Midnight            │
                                 │  Reconciliation      │
                                 │  Engine (Celery)     │
                                 └─────────┬───────────┘
                                           │
                              ┌────────────┴────────────┐
                              │                         │
                    ┌─────────▼──────┐       ┌──────────▼────────┐
                    │  ✅ Reconciled  │       │  ⚠️ Discrepancy    │
                    │  → Reports OK  │       │  → Reports BLOCKED│
                    └────────────────┘       └───────────────────┘
```

## Project Structure

```
verileaf/
├── app/
│   ├── api/main.py              # FastAPI endpoints
│   ├── core/config.py           # Settings, encryption, DB engine
│   ├── models/
│   │   ├── models.py            # SQLAlchemy 2.0 ORM (dual-track ledger)
│   │   └── schemas.py           # Pydantic strict validation
│   ├── services/
│   │   ├── greenline.py         # POS API client + webhook ingestor
│   │   ├── reconciliation.py    # Midnight reconciliation engine
│   │   └── mock_greenline.py    # Test fixtures (no live key needed)
│   ├── reports/
│   │   └── exporter.py          # AGCO CSV + CTLS CSV generators
│   └── worker.py                # Celery beat (scheduled reconciliation)
├── tests/
│   └── test_reconciliation.py   # Unit tests
├── alembic/                     # Database migrations
├── alembic.ini
└── pyproject.toml
```

## Quickstart

```bash
# 1. Generate encryption key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Set environment
export VERILEAF_DATABASE_URL=postgresql+asyncpg://verileaf:verileaf@localhost:5432/verileaf
export VERILEAF_FERNET_KEY=<key from step 1>
export VERILEAF_REDIS_URL=redis://localhost:6379/0

# 3. Run migrations
alembic upgrade head

# 4. Start API
uvicorn app.api.main:app --reload --port 8000

# 5. Start Celery worker (separate terminal)
celery -A app.worker worker --beat --loglevel=info

# 6. Run tests
pytest tests/ -v
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Dual-track ledger | Raw events are immutable (audit trail); snapshots are computed (reports) |
| Idempotent webhooks | `ON CONFLICT DO NOTHING` prevents double-counting on retries |
| Report blocking | Cannot export AGCO/CTLS until ALL discrepancies acknowledged |
| Fernet encryption | API tokens encrypted at rest; key in env, never in DB |
| ca-central-1 only | Canadian data residency requirement |
| ±0.5g tolerance | Configurable; below this, rounding noise is ignored |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/webhooks/greenline` | Ingest POS webhooks (HMAC verified) |
| POST | `/reconcile/{location_id}?report_date=` | Trigger reconciliation |
| GET | `/reports/agco?location_id=&year=&month=` | Download AGCO CSV |
| GET | `/reports/ctls?location_id=&year=&month=` | Download CTLS CSV |
| GET | `/discrepancies?location_id=` | List open discrepancies |
| POST | `/discrepancies/{id}/acknowledge` | Acknowledge discrepancy |
