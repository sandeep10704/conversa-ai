"""Agent domain events for session lifecycle, response suggestions, and user feedback."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.events.base import BaseEvent, utc_now
from app.events.topics import KafkaTopics


class SessionLifecyclePayload(BaseModel):
    """Payload for session creation, update, or termination."""

    model_config = ConfigDict(extra="ignore")

    status: str
    situation: str
    objective: str
    communication_goals: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)


class SessionLifecycleEvent(BaseEvent[SessionLifecyclePayload]):
    """Event emitted when a conversation session lifecycle state changes."""

    event_type: Literal[KafkaTopics.SESSION_EVENTS.value] = (
        KafkaTopics.SESSION_EVENTS.value
    )
    event_version: str = "v1"


class SuggestionItemPayload(BaseModel):
    """Single suggested response option."""

    model_config = ConfigDict(extra="ignore")

    response_type: str
    text: str
    rationale: str
    follow_up_question: str | None = None


class SuggestionPayload(BaseModel):
    """Payload containing generated agent response suggestions."""

    model_config = ConfigDict(extra="ignore")

    status: str
    relevance_confidence: float
    relevance_reason: str
    suggestions: list[SuggestionItemPayload] = Field(default_factory=list)
    selected_model: str | None = None


class SuggestionEvent(BaseEvent[SuggestionPayload]):
    """Event emitted when response suggestions are generated for a session."""

    event_type: Literal[KafkaTopics.AGENT_SUGGESTIONS.value] = (
        KafkaTopics.AGENT_SUGGESTIONS.value
    )
    event_version: str = "v1"


class SuggestionGeneratedPayload(BaseModel):
    """Payload representing a single generated suggestion item ready for delivery."""

    model_config = ConfigDict(extra="ignore")

    session_id: UUID
    suggestion_id: UUID
    execution_id: UUID | None = None
    source_sequence_number: int
    response_type: str
    suggestion_text: str
    rationale: str | None = None
    selected_model: str | None = None
    created_at: datetime = Field(default_factory=utc_now)


class SuggestionGeneratedEvent(BaseEvent[SuggestionGeneratedPayload]):
    """Event emitted to agent.suggestions when an individual suggestion is generated and committed."""

    event_type: Literal[KafkaTopics.AGENT_SUGGESTIONS.value] = (
        KafkaTopics.AGENT_SUGGESTIONS.value
    )
    event_version: str = "v1"


class FeedbackPayload(BaseModel):
    """Payload for user feedback on response suggestions."""

    model_config = ConfigDict(extra="ignore")

    rating: int = Field(ge=1, le=5, description="Usefulness rating from 1 to 5")
    comment: str | None = None
    suggestion_text: str | None = None


class FeedbackEvent(BaseEvent[FeedbackPayload]):
    """Event emitted when user provides feedback on a generated suggestion."""

    event_type: Literal[KafkaTopics.AGENT_FEEDBACK.value] = (
        KafkaTopics.AGENT_FEEDBACK.value
    )
    event_version: str = "v1"
