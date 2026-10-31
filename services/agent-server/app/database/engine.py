"""Asynchronous SQLAlchemy engine configuration and lifecycle management."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
)

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_async_engine: AsyncEngine | None = None


def get_async_engine() -> AsyncEngine:
    """Retrieve or create the singleton AsyncEngine instance."""
    global _async_engine

    if _async_engine is None:
        settings = get_settings()
        url = settings.async_database_url

        engine_kwargs: dict[str, Any] = {
            "echo": False,
            "future": True,
        }

        # PostgreSQL connection pooling parameters
        if "sqlite" not in url:
            engine_kwargs.update({
                "pool_size": settings.database_pool_size,
                "max_overflow": settings.database_max_overflow,
                "pool_timeout": settings.database_pool_timeout,
                "pool_recycle": settings.database_pool_recycle,
                "pool_pre_ping": True,
            })
        else:
            engine_kwargs.update({
                "pool_pre_ping": True,
            })

        logger.info(f"Initializing AsyncEngine for database URL: {url.split('@')[-1] if '@' in url else url}")
        _async_engine = create_async_engine(url, **engine_kwargs)

    return _async_engine


async def dispose_engine() -> None:
    """Gracefully dispose and close the AsyncEngine connection pool."""
    global _async_engine
    if _async_engine is not None:
        logger.info("Disposing database AsyncEngine connection pool...")
        await _async_engine.dispose()
        _async_engine = None
