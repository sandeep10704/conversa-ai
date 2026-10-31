from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TranscriptMessageRequest(BaseModel):

    event_id: UUID = Field(default_factory=uuid4)

    text: str = Field(
        min_length=1,
        max_length=10000
    )

    source: Literal[
        "manual",
        "transcription"
    ] = "manual"


class TranscriptMessageResponse(BaseModel):

    event_id: UUID
    session_id: UUID

    sequence_number: int

    text: str
    source: str

    created_at: datetime