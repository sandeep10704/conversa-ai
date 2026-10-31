"""SQLAlchemy 2.x declarative models for Conversa AI."""

from datetime import datetime, timezone
from typing import Any
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

    # Relationships
    messages: Mapped[list["ConversationMessageModel"]] = relationship(
        "ConversationMessageModel",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="ConversationMessageModel.sequence_number",
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
