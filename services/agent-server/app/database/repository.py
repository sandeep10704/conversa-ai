"""Asynchronous repositories for Sessions and Conversation Messages."""

from datetime import datetime
import logging
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import (
    AgentExecutionModel,
    ConversationMessageModel,
    OutboxEventModel,
    ProcessedEventModel,
    SessionModel,
    SuggestionModel,
    utc_now,
)
from app.events.transcript import (
    TranscriptFinalEvent,
    TranscriptFinalPayload,
)

from app.schemas.conversation import (
    TranscriptMessageRequest,
    TranscriptMessageResponse,
)
from app.schemas.session import (
    CreateSessionRequest,
    ParticipantContext,
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
        participant_data = (
            request.participant_context.model_dump()
            if request.participant_context is not None
            else None
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
            participant_context=participant_data,
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
            participant_context=ParticipantContext(**db_session.participant_context) if db_session.participant_context else None,
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
            participant_context=ParticipantContext(**db_session.participant_context) if db_session.participant_context else None,
        )

    async def update_participant_context(
        self, session_id: UUID, context: Optional[ParticipantContext]
    ) -> SessionResponse | None:
        """Update or clear participant context on an active session."""
        stmt = select(SessionModel).where(SessionModel.session_id == session_id)
        result = await self.db.execute(stmt)
        db_session = result.scalar_one_or_none()
        if db_session is None:
            return None

        db_session.participant_context = context.model_dump() if context is not None else None
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
            participant_context=ParticipantContext(**db_session.participant_context) if db_session.participant_context else None,
        )

    async def end_session(self, session_id: UUID) -> SessionResponse | None:
        """Transition session to 'ended' status and explicitly delete participant_context for privacy."""
        stmt = select(SessionModel).where(SessionModel.session_id == session_id)
        result = await self.db.execute(stmt)
        db_session = result.scalar_one_or_none()
        if db_session is None:
            return None

        db_session.status = "ended"
        db_session.participant_context = None  # Explicit context deletion on session termination!
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
            participant_context=None,
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

        # 4. Create and persist message AND outbox event in the same transaction
        db_message = ConversationMessageModel(
            message_id=request.event_id,
            session_id=session_id,
            sequence_number=next_seq,
            text=request.text,
            source=request.source,
            created_at=utc_now(),
        )

        event = TranscriptFinalEvent(
            event_id=db_message.message_id,
            session_id=session_id,
            sequence_number=next_seq,
            occurred_at=db_message.created_at,
            payload=TranscriptFinalPayload(
                sequence_number=next_seq,
                text=db_message.text,
                source=db_message.source,  # type: ignore[arg-type]
            ),
        )

        outbox_event = OutboxEventModel(
            event_id=event.event_id,
            aggregate_id=session_id,
            event_type=event.event_type,
            event_version=event.event_version,
            payload=event.model_dump(mode="json"),
            status="pending",
            created_at=db_message.created_at,
        )

        self.db.add(db_message)
        self.db.add(outbox_event)
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


class OutboxRepository:
    """Asynchronous repository for Transactional Outbox pattern operations."""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def fetch_pending_events(self, batch_size: int = 50) -> list[OutboxEventModel]:
        """Fetch pending outbox events FIFO, claiming rows with SKIP LOCKED on PostgreSQL."""
        bind = self.db.bind
        dialect_name = bind.dialect.name if bind else ""

        stmt = (
            select(OutboxEventModel)
            .where(OutboxEventModel.status == "pending")
            .order_by(OutboxEventModel.created_at.asc())
            .limit(batch_size)
        )

        if dialect_name == "postgresql":
            # Concurrency safety: other outbox publisher workers will skip rows currently locked
            stmt = stmt.with_for_update(skip_locked=True)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def mark_published(self, event_id: UUID) -> None:
        """Mark an outbox event as successfully published."""
        stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == event_id)
        res = await self.db.execute(stmt)
        event = res.scalar_one_or_none()
        if event is not None:
            event.status = "published"
            event.published_at = utc_now()
            event.publish_attempts += 1
            event.last_error = None
            await self.db.flush()

    async def record_failure(
        self,
        event_id: UUID,
        error: str,
        max_retries: int = 5,
    ) -> None:
        """Record a publishing failure with attempt count and error message."""
        stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == event_id)
        res = await self.db.execute(stmt)
        event = res.scalar_one_or_none()
        if event is not None:
            event.publish_attempts += 1
            event.last_error = error[:500]
            if event.publish_attempts >= max_retries:
                event.status = "failed"
            else:
                event.status = "pending"
            await self.db.flush()

    async def get_counts(self) -> dict[str, int]:
        """Get counts of outbox events by status for observability."""
        stmt = (
            select(OutboxEventModel.status, func.count(OutboxEventModel.event_id))
            .group_by(OutboxEventModel.status)
        )
        res = await self.db.execute(stmt)
        counts = dict(res.all())
        return {
            "pending": counts.get("pending", 0),
            "published": counts.get("published", 0),
            "failed": counts.get("failed", 0),
        }


class ProcessedEventRepository:
    """Asynchronous repository for consumer idempotency tracking."""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def is_processed(self, event_id: UUID, consumer_id: str = "default") -> bool:
        """Check if an event has already been successfully processed by this consumer."""
        stmt = select(ProcessedEventModel.event_id).where(
            ProcessedEventModel.event_id == event_id,
            ProcessedEventModel.consumer_id == consumer_id,
        )
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none() is not None

    async def mark_processed(
        self,
        event_id: UUID,
        event_type: str,
        session_id: UUID,
        consumer_id: str = "default",
    ) -> None:
        """Record an event as processed for deduplication."""
        record = ProcessedEventModel(
            event_id=event_id,
            consumer_id=consumer_id,
            event_type=event_type,
            session_id=session_id,
            processed_at=utc_now(),
        )
        self.db.add(record)
        await self.db.flush()


class AgentExecutionRepository:
    """Asynchronous repository for managing agent execution lifecycle and state."""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def create_execution(
        self,
        session_id: UUID,
        trigger_reason: str,
        processed_sequence_number: int,
        selected_model: str | None = None,
    ) -> AgentExecutionModel:
        """Create and persist a new running agent execution record."""
        execution = AgentExecutionModel(
            execution_id=uuid4(),
            session_id=session_id,
            trigger_reason=trigger_reason,
            processed_sequence_number=processed_sequence_number,
            selected_model=selected_model,
            execution_status="running",
            created_at=utc_now(),
        )
        self.db.add(execution)
        await self.db.flush()
        return execution

    async def update_status(
        self,
        execution_id: UUID,
        status: str,
        failure_stage: str | None = None,
        selected_model: str | None = None,
        completed_at: datetime | None = None,
    ) -> AgentExecutionModel | None:
        """Update the status, failure stage, and completion time of an execution."""
        stmt = select(AgentExecutionModel).where(AgentExecutionModel.execution_id == execution_id)
        res = await self.db.execute(stmt)
        execution = res.scalar_one_or_none()
        if execution is not None:
            execution.execution_status = status
            if failure_stage is not None:
                execution.failure_stage = failure_stage
            if selected_model is not None:
                execution.selected_model = selected_model
            execution.completed_at = completed_at or utc_now()
            await self.db.flush()
        return execution

    async def has_active_execution(self, session_id: UUID) -> bool:
        """Check if there is currently an in-progress execution for this session."""
        stmt = (
            select(AgentExecutionModel.execution_id)
            .where(
                AgentExecutionModel.session_id == session_id,
                AgentExecutionModel.execution_status == "running",
            )
            .limit(1)
        )
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none() is not None

    async def get_latest_processed_sequence(self, session_id: UUID) -> int:
        """Get the highest sequence number processed in completed executions for a session."""
        stmt = (
            select(func.coalesce(func.max(AgentExecutionModel.processed_sequence_number), 0))
            .where(
                AgentExecutionModel.session_id == session_id,
                AgentExecutionModel.execution_status.in_(["completed", "no_suggestion"]),
            )
        )
        res = await self.db.execute(stmt)
        return int(res.scalar() or 0)

    async def get_last_execution_time(self, session_id: UUID) -> datetime | None:
        """Get the start time of the most recent execution for this session."""
        stmt = (
            select(AgentExecutionModel.created_at)
            .where(AgentExecutionModel.session_id == session_id)
            .order_by(AgentExecutionModel.created_at.desc())
            .limit(1)
        )
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()

    async def get_execution(self, execution_id: UUID) -> AgentExecutionModel | None:
        """Retrieve execution by UUID."""
        stmt = select(AgentExecutionModel).where(AgentExecutionModel.execution_id == execution_id)
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()


class SuggestionRepository:
    """Asynchronous repository for persisting agent suggestions and transactional outbox events."""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def create_suggestion_atomic_with_outbox(
        self,
        session_id: UUID,
        execution_id: UUID | None,
        source_last_sequence_number: int,
        response_type: str,
        suggestion_text: str,
        rationale: str | None = None,
        selected_model: str | None = None,
    ) -> tuple[SuggestionModel, OutboxEventModel]:
        """Atomically insert a Suggestion row and an Outbox event in the same transaction."""
        suggestion_id = uuid4()
        now = utc_now()

        suggestion = SuggestionModel(
            suggestion_id=suggestion_id,
            session_id=session_id,
            execution_id=execution_id,
            source_last_sequence_number=source_last_sequence_number,
            response_type=response_type,
            suggestion_text=suggestion_text,
            rationale=rationale,
            created_at=now,
            delivery_status="pending",
        )
        self.db.add(suggestion)

        outbox_event = OutboxEventModel(
            event_id=uuid4(),
            aggregate_id=session_id,
            event_type="agent.suggestions",
            payload={
                "event_id": str(uuid4()),
                "event_type": "agent.suggestions",
                "event_version": "v1",
                "aggregate_id": str(session_id),
                "timestamp": now.isoformat(),
                "payload": {
                    "session_id": str(session_id),
                    "suggestion_id": str(suggestion_id),
                    "execution_id": str(execution_id) if execution_id else None,
                    "source_sequence_number": source_last_sequence_number,
                    "response_type": response_type,
                    "suggestion_text": suggestion_text,
                    "rationale": rationale,
                    "selected_model": selected_model,
                    "created_at": now.isoformat(),
                },
            },
            created_at=now,
            status="pending",
        )
        self.db.add(outbox_event)
        await self.db.flush()
        return suggestion, outbox_event

    async def get_suggestions_for_session(
        self, session_id: UUID, limit: int = 50
    ) -> list[SuggestionModel]:
        """Get suggestions for a session ordered by creation time descending."""
        stmt = (
            select(SuggestionModel)
            .where(SuggestionModel.session_id == session_id)
            .order_by(SuggestionModel.created_at.desc())
            .limit(limit)
        )
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def mark_delivered(self, suggestion_id: UUID) -> None:
        """Mark a suggestion as delivered to WebSocket client."""
        stmt = select(SuggestionModel).where(SuggestionModel.suggestion_id == suggestion_id)
        res = await self.db.execute(stmt)
        suggestion = res.scalar_one_or_none()
        if suggestion is not None:
            suggestion.delivery_status = "delivered"
            await self.db.flush()
