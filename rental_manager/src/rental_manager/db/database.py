"""Database connection and session management."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from rental_manager.config import settings
from rental_manager.db.models import Base

logger = logging.getLogger(__name__)

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    connect_args={"timeout": 30},
)

async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Initialize the database, creating all tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _run_migrations()


async def _run_migrations() -> None:
    """Run lightweight schema migrations for new columns on existing tables."""
    migrations = [
        "ALTER TABLE bookings ADD COLUMN code_disabled BOOLEAN DEFAULT 0",
        "ALTER TABLE bookings ADD COLUMN code_disabled_at DATETIME",
        "ALTER TABLE calendars ADD COLUMN ha_entity_id VARCHAR(100)",
        "ALTER TABLE calendars ADD COLUMN hosttools_listing_id VARCHAR(50)",
        "ALTER TABLE locks ADD COLUMN auto_lock_enabled BOOLEAN",
        "ALTER TABLE audit_log ADD COLUMN batch_id VARCHAR(50)",
        "ALTER TABLE locks ADD COLUMN volume_level VARCHAR(10)",
    ]
    async with engine.begin() as conn:
        for stmt in migrations:
            try:
                await conn.execute(text(stmt))
                logger.info("Migration applied: %s", stmt)
            except Exception:
                pass  # Column already exists

        # Backfill ha_entity_id and hosttools_listing_id for existing calendars
        try:
            from rental_manager.config import _CALENDAR_META
            for cal_id, (_, _, _, ha_entity_id, hosttools_listing_id) in _CALENDAR_META.items():
                await conn.execute(text(
                    "UPDATE calendars SET ha_entity_id = :ha_entity_id "
                    "WHERE calendar_id = :cal_id AND (ha_entity_id IS NULL OR ha_entity_id = 'calendar.' || calendar_id)"
                ), {"ha_entity_id": ha_entity_id, "cal_id": cal_id})
                await conn.execute(text(
                    "UPDATE calendars SET hosttools_listing_id = :listing_id "
                    "WHERE calendar_id = :cal_id AND hosttools_listing_id IS NULL"
                ), {"listing_id": hosttools_listing_id, "cal_id": cal_id})
            logger.info("Backfilled ha_entity_id and hosttools_listing_id for existing calendars")
        except Exception:
            pass


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session."""
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_session_context() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session as a context manager."""
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
