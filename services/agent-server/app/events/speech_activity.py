"""Speech activity event schema for real-time speech onset and resumption notifications."""

from typing import Literal, Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

from app.events.base import BaseEvent
from app.events.topics import KafkaTopics


class SpeechActivityPayload(BaseModel):
    """Payload for real-time VAD speech activity transitions."""

    model_config = ConfigDict(extra="ignore")

    activity_type: Literal["speech_onset", "speech_resumed"] = Field(
        default="speech_onset",
        description="Type of speech activity transition",
    )
    activity_sequence: int = Field(
        ge=1,
        description="Monotonic activity sequence number within the session",
    )
    timestamp: float = Field(
        description="Epoch timestamp in seconds of the VAD speech onset transition",
    )
    source_sequence: Optional[int] = Field(
        default=None,
        description="Unified monotonic source sequence across transcript and activity events",
    )
    last_finalized_sequence: Optional[int] = Field(
        default=None,
        description="Highest finalized transcript sequence number at onset time",
    )



class SpeechActivityEvent(BaseEvent[SpeechActivityPayload]):
    """Event published when VAD detects speech onset to abort pending turn-completion timers."""

    event_type: Literal[KafkaTopics.SPEECH_ACTIVITY.value] = (
        KafkaTopics.SPEECH_ACTIVITY.value
    )
    event_version: str = "v1"
