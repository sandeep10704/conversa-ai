"""Session store backed by PostgreSQL repository."""

from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.repository import SessionRepository, SuggestionRepository
from app.database.session import async_session_factory, get_db_session
from app.schemas.session import (
    CreateSessionRequest,
    SessionResponse,
)


async def get_session_repository(
    session: AsyncSession = Depends(get_db_session),
) -> SessionRepository:
    """FastAPI dependency for SessionRepository."""
    return SessionRepository(session)


async def get_suggestion_repository(
    session: AsyncSession = Depends(get_db_session),
) -> SuggestionRepository:
    """FastAPI dependency for SuggestionRepository."""
    return SuggestionRepository(session)


class SessionStore:
    """Session store interface delegating to SessionRepository."""

    @staticmethod
    async def create(
        request: CreateSessionRequest,
        db: AsyncSession | None = None,
    ) -> SessionResponse:
        """Create a new session in PostgreSQL."""
        if db is not None:
            repo = SessionRepository(db)
            return await repo.create(request)

        async with async_session_factory()() as session:
            async with session.begin():
                repo = SessionRepository(session)
                return await repo.create(request)

    @staticmethod
    async def get(
        session_id: UUID,
        db: AsyncSession | None = None,
    ) -> SessionResponse | None:
        """Retrieve a session from PostgreSQL."""
        if db is not None:
            repo = SessionRepository(db)
            return await repo.get(session_id)

        async with async_session_factory()() as session:
            repo = SessionRepository(session)
            return await repo.get(session_id)

    @staticmethod
    async def delete(
        session_id: UUID,
        db: AsyncSession | None = None,
    ) -> bool:
        """Delete a session from PostgreSQL."""
        if db is not None:
            repo = SessionRepository(db)
            return await repo.delete(session_id)

        async with async_session_factory()() as session:
            async with session.begin():
                repo = SessionRepository(session)
                return await repo.delete(session_id)


session_store = SessionStore()
