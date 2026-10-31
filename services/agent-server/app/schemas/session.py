
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class ResponsePreferences(BaseModel):
    style: Literal[
        "natural",
        "professional",
        "casual",
        "technical"
    ] = "natural"

    length: Literal[
        "short",
        "concise",
        "detailed"
    ] = "concise"

    technical_level: Literal[
        "beginner",
        "intermediate",
        "advanced"
    ] = "intermediate"


class CreateSessionRequest(BaseModel):
    situation: str = Field(min_length=5, max_length=3000)

    objective: str = Field(min_length=5, max_length=3000)

    communication_goals: list[str] = Field(
        default_factory=list,
        max_length=10
    )

    response_preferences: ResponsePreferences = Field(
        default_factory=ResponsePreferences
    )

    constraints: list[str] = Field(
        default_factory=list,
        max_length=20
    )


class SessionResponse(BaseModel):
    session_id: UUID
    status: Literal["active", "ended"]
    created_at: datetime

    situation: str
    objective: str
    communication_goals: list[str]
    response_preferences: ResponsePreferences
    constraints: list[str]
