"""Dead Letter Queue (DLQ) event schemas for permanently failed messages."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .base import BaseEvent, utc_now
from .topics import KafkaTopics



class DLQPayload(BaseModel):
    """Payload for capturing failed events routed to the Dead Letter Queue."""

    model_config = ConfigDict(extra="ignore")

    original_event_id: str | None = None
    original_topic: str
    error_stage: str
    error_message: str
    retry_count: int = 0
    raw_payload: Any = None
    failed_at: datetime = Field(default_factory=utc_now)


class DLQEvent(BaseEvent[DLQPayload]):
    """Event envelope for dead-lettered messages."""

    event_type: Literal[KafkaTopics.AGENT_DLQ.value] = (
        KafkaTopics.AGENT_DLQ.value
    )
    event_version: str = "v1"
