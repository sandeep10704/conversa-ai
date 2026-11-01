"""Agent command event schemas for orchestrating agent triggers."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .base import BaseEvent, utc_now
from .topics import KafkaTopics


class AgentCommandPayload(BaseModel):
    """Payload instructing an agent worker to execute for a session."""

    model_config = ConfigDict(extra="ignore")

    session_id: UUID = Field(description="Target session UUID")
    trigger_reason: str = Field(
        default="min_messages",
        description="Reason trigger fired: min_messages, debounce, manual, etc.",
    )
    processed_sequence_number: int = Field(
        ge=0,
        description="Highest transcript sequence number evaluated at trigger time",
    )
    message_count: int = Field(
        default=0,
        ge=0,
        description="Number of messages accumulated since last trigger",
    )
    created_at: datetime = Field(default_factory=utc_now)


class AgentCommandEvent(BaseEvent[AgentCommandPayload]):
    """Event published to agent.commands to dispatch an agent inference execution."""

    event_type: Literal[KafkaTopics.AGENT_COMMANDS.value] = (
        KafkaTopics.AGENT_COMMANDS.value
    )
    event_version: str = "v1"
