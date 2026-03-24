"""
VeriLeaf Core Configuration
- Pydantic-Settings for 12-factor config
- Fernet encryption for API tokens at rest
- Async SQLAlchemy engine factory
"""
from functools import lru_cache
from pydantic_settings import BaseSettings
from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession


class Settings(BaseSettings):
    # Database (MUST be ca-central-1)
    database_url: str = "postgresql+asyncpg://verileaf:verileaf@localhost:5432/verileaf"

    # Redis / Celery
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"

    # Greenline POS
    greenline_base_url: str = "https://api.getgreenline.co/v1"
    greenline_webhook_secret: str = ""

    # Encryption (generate once: Fernet.generate_key().decode())
    fernet_key: str = ""

    # Compliance
    reconciliation_tolerance_grams: float = 0.5  # ±0.5g before flagging
    midnight_cron_hour: int = 23
    midnight_cron_minute: int = 59

    model_config = {"env_prefix": "VERILEAF_", "env_file": ".env"}


@lru_cache
def get_settings() -> Settings:
    return Settings()


# ---------------------------------------------------------------------------
# Encryption helpers — all POS API tokens encrypted at rest via Fernet
# ---------------------------------------------------------------------------

def get_fernet() -> Fernet:
    return Fernet(get_settings().fernet_key.encode())


def encrypt_token(plaintext: str) -> str:
    return get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> str:
    return get_fernet().decrypt(ciphertext.encode()).decode()


# ---------------------------------------------------------------------------
# Async DB engine
# ---------------------------------------------------------------------------

def build_engine(url: str | None = None):
    return create_async_engine(
        url or get_settings().database_url,
        echo=False,
        pool_size=10,
        max_overflow=20,
    )


engine = build_engine()
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncSession:  # FastAPI Depends()
    async with AsyncSessionLocal() as session:
        yield session
