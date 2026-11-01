"""Base event model and schema evolution rules for Conversa AI."""

from datetime import datetime, timezone
from typing import Any, Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    """Return timezone-aware current UTC datetime."""
    return datetime.now(timezone.utc)


PayloadT = TypeVar("PayloadT")


class BaseEvent(BaseModel, Generic[PayloadT]):
    """Standard envelope for all Conversa AI Kafka events.

    Attributes:
        event_id: Globally unique identifier for this event occurrence.
        event_type: Topic-aligned event type name (e.g. 'conversation.transcript.final').
        event_version: Schema version string (e.g. 'v1').
        session_id: Target conversation session UUID. Serves as Kafka partition key.
        occurred_at: UTC timestamp when the event originated.
        payload: Strongly-typed event-specific payload body.
    """

    model_config = ConfigDict(extra="ignore")

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    event_version: str = "v1"
    session_id: UUID
    occurred_at: datetime = Field(default_factory=utc_now)
    payload: PayloadT


class EventCompatibility:
    """Schema compatibility rules for Conversa AI event evolution:

    1. Backward Compatibility:
       - Old consumers MUST be able to process new events.
       - Never delete required fields; make old fields optional with defaults.
       - Unknown fields MUST be ignored (`extra='ignore'`).

    2. Forward Compatibility:
       - New consumers MUST be able to process old events.
       - Any newly added field MUST have a default value or be optional.

    3. Version Bumping Rules:
       - Minor additions (new optional fields) retain the existing version (e.g. 'v1').
       - Breaking changes (field semantic changes, type alterations) require a new version
         (e.g. 'v2') published alongside or with a phased consumer transition period.

    4. Partition Keying:
       - All events associated with a conversation session MUST use `session_id` as the
         Kafka message key to guarantee partition-level FIFO ordering.
    """

    @staticmethod
    def is_compatible_version(event_version: str, supported_versions: tuple[str, ...]) -> bool:
        """Check whether incoming event version is supported."""
        return event_version in supported_versions
