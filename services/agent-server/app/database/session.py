"""Asynchronous session factory and FastAPI dependency injection."""

from collections.abc import AsyncGenerator
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)

from app.database.engine import get_async_engine

logger = logging.getLogger(__name__)


def async_session_factory() -> async_sessionmaker[AsyncSession]:
    """Create an async sessionmaker bound to the engine."""
    engine = get_async_engine()
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for per-request database sessions.

    Guarantees session lifecycle:
    - Dedicated per-request AsyncSession (no global session).
    - Commits transaction on success, rolls back on unhandled error.
    - Always closes session upon completion.
    """
    session_maker = async_session_factory()
    async with session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def check_db_health() -> tuple[bool, str]:
    """Perform a quick health ping on the database connection.

    Returns:
        (is_healthy, status_message)
    """
    try:
        session_maker = async_session_factory()
        async with session_maker() as session:
            result = await session.execute(text("SELECT 1"))
            val = result.scalar()
            if val == 1:
                return True, "connected"
            return False, f"Unexpected health check response: {val}"
    except Exception as exc:
        logger.warning(f"Database health check failed: {exc}")
        return False, str(exc)
