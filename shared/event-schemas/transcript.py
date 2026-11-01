"""Transcript event schemas for final and partial conversation utterances."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .base import BaseEvent, utc_now
from .topics import KafkaTopics



class TranscriptFinalPayload(BaseModel):
    """Payload for completed conversation messages."""

    model_config = ConfigDict(extra="ignore")

    sequence_number: int = Field(ge=1, description="Monotonic sequence number within the session")
    text: str = Field(min_length=1, description="Full utterance text")
    source: Literal["manual", "transcription"] = Field(
        default="manual", description="Origin source of message"
    )


class TranscriptFinalEvent(BaseEvent[TranscriptFinalPayload]):
    """Event published when a final transcript utterance is committed."""

    event_type: Literal[KafkaTopics.TRANSCRIPT_FINAL.value] = (
        KafkaTopics.TRANSCRIPT_FINAL.value
    )
    event_version: str = "v1"
    sequence_number: int = Field(ge=1, description="Top-level sequence number for quick routing")


class TranscriptPartialPayload(BaseModel):
    """Payload for interim/partial utterances streaming from audio transcription."""

    model_config = ConfigDict(extra="ignore")

    text: str = Field(min_length=1, description="Interim hypothesis text")
    source: Literal["manual", "transcription"] = Field(
        default="transcription", description="Origin source of message"
    )
    is_final: bool = Field(default=False, description="Flag indicating if utterance finalized")


class TranscriptPartialEvent(BaseEvent[TranscriptPartialPayload]):
    """Event published during live streaming partial transcript generation."""

    event_type: Literal[KafkaTopics.TRANSCRIPT_PARTIAL.value] = (
        KafkaTopics.TRANSCRIPT_PARTIAL.value
    )
    event_version: str = "v1"
