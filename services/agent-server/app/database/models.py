"""SQLAlchemy 2.x declarative models for Conversa AI."""

from datetime import datetime, timezone
from typing import Any, Optional
import uuid

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)
from sqlalchemy.types import JSON


def utc_now() -> datetime:
    """Return timezone-aware current UTC datetime."""
    return datetime.now(timezone.utc)


# Compatible JSON type: native JSONB on PostgreSQL, standard JSON elsewhere
JSON_TYPE = JSON().with_variant(JSONB, "postgresql")


class Base(DeclarativeBase):
    """SQLAlchemy Declarative Base."""
    pass


class SessionModel(Base):
    """Database model for Conversa AI conversation sessions."""

    __tablename__ = "sessions"

    session_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="active",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )

    situation: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    objective: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    communication_goals: Mapped[list[str]] = mapped_column(
        JSON_TYPE,
        nullable=False,
        default=list,
    )

    response_preferences: Mapped[dict[str, Any]] = mapped_column(
        JSON_TYPE,
        nullable=False,
        default=dict,
    )

    constraints: Mapped[list[str]] = mapped_column(
        JSON_TYPE,
        nullable=False,
        default=list,
    )

    participant_context: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON_TYPE,
        nullable=True,
        default=None,
    )

    # Relationships
    messages: Mapped[list["ConversationMessageModel"]] = relationship(
        "ConversationMessageModel",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="ConversationMessageModel.sequence_number",
        lazy="selectin",
    )

    executions: Mapped[list["AgentExecutionModel"]] = relationship(
        "AgentExecutionModel",
        back_populates="session",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    suggestions: Mapped[list["SuggestionModel"]] = relationship(
        "SuggestionModel",
        back_populates="session",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ConversationMessageModel(Base):
    """Database model for chronological conversation transcript messages."""

    __tablename__ = "conversation_messages"

    message_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("sessions.session_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    sequence_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    text: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    source: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="manual",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )

    # Relationships
    session: Mapped["SessionModel"] = relationship(
        "SessionModel",
        back_populates="messages",
    )

    __table_args__ = (
        UniqueConstraint("session_id", "sequence_number", name="uq_session_sequence"),
        Index("ix_messages_session_id", "session_id"),
    )


class OutboxEventModel(Base):
    """Transactional Outbox event model for reliable event publishing."""

    __tablename__ = "outbox_events"

    event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
        index=True,
    )

    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
    )

    event_version: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="v1",
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON_TYPE,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True,
    )

    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    publish_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="pending",
        index=True,
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    __table_args__ = (
        Index("ix_outbox_pending_fifo", "status", "created_at"),
        Index("ix_outbox_aggregate", "aggregate_id", "created_at"),
    )


class ProcessedEventModel(Base):
    """Consumer idempotency tracking model for duplicate detection.

    Uses a composite primary key (event_id, consumer_id) to ensure independent
    consumer groups (e.g. trigger_controller, agent_worker, delivery_service)
    do not collide or falsely skip unconsumed events.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
    )

    consumer_id: Mapped[str] = mapped_column(
        String(100),
        primary_key=True,
        default="default",
    )

    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
        index=True,
    )

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )


class AgentExecutionModel(Base):
    """Tracks agent execution runs, status, and telemetry per session."""

    __tablename__ = "agent_executions"

    execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("sessions.session_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    trigger_reason: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="auto_trigger",
    )

    processed_sequence_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    selected_model: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    execution_status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="pending",
        index=True,
    )

    failure_stage: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    session: Mapped["SessionModel"] = relationship(
        "SessionModel",
        back_populates="executions",
    )

    suggestions: Mapped[list["SuggestionModel"]] = relationship(
        "SuggestionModel",
        back_populates="execution",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_agent_executions_session_status", "session_id", "execution_status"),
        Index("ix_agent_executions_session_seq", "session_id", "processed_sequence_number"),
    )


class SuggestionModel(Base):
    """Persisted generated suggestions with delivery and provenance metadata."""

    __tablename__ = "suggestions"

    suggestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("sessions.session_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    execution_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("agent_executions.execution_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    source_last_sequence_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    response_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    suggestion_text: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    rationale: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )

    delivery_status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="pending",
    )

    # Relationships
    session: Mapped["SessionModel"] = relationship(
        "SessionModel",
        back_populates="suggestions",
    )

    execution: Mapped["AgentExecutionModel | None"] = relationship(
        "AgentExecutionModel",
        back_populates="suggestions",
    )

    __table_args__ = (
        Index("ix_suggestions_session_created", "session_id", "created_at"),
    )
