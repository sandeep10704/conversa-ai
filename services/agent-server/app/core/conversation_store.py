"""Conversation store backed by PostgreSQL repository."""

from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.repository import (
    ConversationRepository,
    SessionNotFoundError,
)
from app.database.session import async_session_factory, get_db_session
from app.schemas.conversation import (
    TranscriptMessageRequest,
    TranscriptMessageResponse,
)


async def get_conversation_repository(
    session: AsyncSession = Depends(get_db_session),
) -> ConversationRepository:
    """FastAPI dependency for ConversationRepository."""
    return ConversationRepository(session)


class ConversationStore:
    """Conversation store interface delegating to ConversationRepository."""

    @staticmethod
    async def add_message(
        session_id: UUID,
        request: TranscriptMessageRequest,
        db: AsyncSession | None = None,
    ) -> TranscriptMessageResponse:
        """Append a message to PostgreSQL session."""
        if db is not None:
            repo = ConversationRepository(db)
            return await repo.add_message(session_id, request)

        async with async_session_factory()() as session:
            async with session.begin():
                repo = ConversationRepository(session)
                return await repo.add_message(session_id, request)

    @staticmethod
    async def get_messages(
        session_id: UUID,
        db: AsyncSession | None = None,
    ) -> list[TranscriptMessageResponse]:
        """Retrieve chronological messages for a session from PostgreSQL."""
        if db is not None:
            repo = ConversationRepository(db)
            return await repo.get_messages(session_id)

        async with async_session_factory()() as session:
            repo = ConversationRepository(session)
            return await repo.get_messages(session_id)


conversation_store = ConversationStore()