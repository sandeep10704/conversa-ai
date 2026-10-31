"""Asynchronous repositories for Sessions and Conversation Messages."""

import logging
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import (
    ConversationMessageModel,
    SessionModel,
    utc_now,
)
from app.schemas.conversation import (
    TranscriptMessageRequest,
    TranscriptMessageResponse,
)
from app.schemas.session import (
    CreateSessionRequest,
    ResponsePreferences,
    SessionResponse,
)

logger = logging.getLogger(__name__)


class SessionNotFoundError(Exception):
    """Raised when an operation targets a nonexistent session."""
    pass


class SessionRepository:
    """Asynchronous repository for Conversa AI sessions."""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def create(self, request: CreateSessionRequest) -> SessionResponse:
        """Create and persist a new session."""
        session_id = uuid4()
        created_at = utc_now()

        preferences_data = (
            request.response_preferences.model_dump()
            if hasattr(request.response_preferences, "model_dump")
            else dict(request.response_preferences)
        )

        db_session = SessionModel(
            session_id=session_id,
            status="active",
            created_at=created_at,
            situation=request.situation,
            objective=request.objective,
            communication_goals=list(request.communication_goals),
            response_preferences=preferences_data,
            constraints=list(request.constraints),
        )

        self.db.add(db_session)
        await self.db.flush()

        return SessionResponse(
            session_id=db_session.session_id,
            status=db_session.status,  # type: ignore[arg-type]
            created_at=db_session.created_at,
            situation=db_session.situation,
            objective=db_session.objective,
            communication_goals=db_session.communication_goals,
            response_preferences=ResponsePreferences(**db_session.response_preferences),
            constraints=db_session.constraints,
        )

    async def get(self, session_id: UUID) -> SessionResponse | None:
        """Retrieve a session by its UUID."""
        stmt = select(SessionModel).where(SessionModel.session_id == session_id)
        result = await self.db.execute(stmt)
        db_session = result.scalar_one_or_none()

        if db_session is None:
            return None

        return SessionResponse(
            session_id=db_session.session_id,
            status=db_session.status,  # type: ignore[arg-type]
            created_at=db_session.created_at,
            situation=db_session.situation,
            objective=db_session.objective,
            communication_goals=db_session.communication_goals,
            response_preferences=ResponsePreferences(**db_session.response_preferences),
            constraints=db_session.constraints,
        )

    async def delete(self, session_id: UUID) -> bool:
        """Delete a session by UUID (cascade deletes associated messages)."""
        stmt = delete(SessionModel).where(SessionModel.session_id == session_id)
        result = await self.db.execute(stmt)
        await self.db.flush()
        return (result.rowcount or 0) > 0


class ConversationRepository:
    """Asynchronous repository for conversation transcript messages."""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def add_message(
        self,
        session_id: UUID,
        request: TranscriptMessageRequest,
    ) -> TranscriptMessageResponse:
        """Atomically append a conversation message to a session.

        Validates session existence, handles idempotent deduplication by event_id,
        and assigns the next sequential sequence_number within the session.
        """
        # 1. Verify session exists (lock row if PostgreSQL to avoid concurrent sequence conflicts)
        bind = self.db.bind
        dialect_name = bind.dialect.name if bind else ""

        session_stmt = select(SessionModel.session_id).where(SessionModel.session_id == session_id)
        if dialect_name == "postgresql":
            session_stmt = session_stmt.with_for_update()

        session_res = await self.db.execute(session_stmt)
        if session_res.scalar_one_or_none() is None:
            raise SessionNotFoundError(f"Session {session_id} not found")

        # 2. Deduplication check: check if event_id already exists in this session
        existing_stmt = select(ConversationMessageModel).where(
            ConversationMessageModel.session_id == session_id,
            ConversationMessageModel.message_id == request.event_id,
        )
        existing_res = await self.db.execute(existing_stmt)
        existing_msg = existing_res.scalar_one_or_none()

        if existing_msg is not None:
            return TranscriptMessageResponse(
                event_id=existing_msg.message_id,
                session_id=existing_msg.session_id,
                sequence_number=existing_msg.sequence_number,
                text=existing_msg.text,
                source=existing_msg.source,
                created_at=existing_msg.created_at,
            )

        # 3. Compute next sequence number atomically
        seq_stmt = select(func.coalesce(func.max(ConversationMessageModel.sequence_number), 0)).where(
            ConversationMessageModel.session_id == session_id
        )
        seq_res = await self.db.execute(seq_stmt)
        max_seq = seq_res.scalar_one()
        next_seq = max_seq + 1

        # 4. Create and persist message
        db_message = ConversationMessageModel(
            message_id=request.event_id,
            session_id=session_id,
            sequence_number=next_seq,
            text=request.text,
            source=request.source,
            created_at=utc_now(),
        )

        self.db.add(db_message)
        await self.db.flush()

        return TranscriptMessageResponse(
            event_id=db_message.message_id,
            session_id=db_message.session_id,
            sequence_number=db_message.sequence_number,
            text=db_message.text,
            source=db_message.source,
            created_at=db_message.created_at,
        )

    async def get_messages(self, session_id: UUID) -> list[TranscriptMessageResponse]:
        """Retrieve all messages for a session ordered chronologically by sequence_number."""
        # Check session exists
        session_stmt = select(SessionModel.session_id).where(SessionModel.session_id == session_id)
        session_res = await self.db.execute(session_stmt)
        if session_res.scalar_one_or_none() is None:
            raise SessionNotFoundError(f"Session {session_id} not found")

        stmt = (
            select(ConversationMessageModel)
            .where(ConversationMessageModel.session_id == session_id)
            .order_by(ConversationMessageModel.sequence_number.asc())
        )
        result = await self.db.execute(stmt)
        messages = result.scalars().all()

        return [
            TranscriptMessageResponse(
                event_id=msg.message_id,
                session_id=msg.session_id,
                sequence_number=msg.sequence_number,
                text=msg.text,
                source=msg.source,
                created_at=msg.created_at,
            )
            for msg in messages
        ]
