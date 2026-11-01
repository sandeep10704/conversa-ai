"""Participant Context schemas for session-specific user context in Conversa AI."""

from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class ParticipantContext(BaseModel):
    """User-provided participant context for a single conversation session.

    Scope:
    - Belongs strictly to the single Conversa AI operator for the duration of one session.
    - Cumulative length across all fields is capped at 1,500 characters.
    - Explicitly deleted upon session termination.
    - Never persisted as a cross-session user profile.
    """

    preferred_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Preferred name by which the user is addressed (e.g. 'Sandeep')",
    )
    role: Optional[str] = Field(
        default=None,
        max_length=150,
        description="Professional role or title (e.g. 'Backend Developer')",
    )
    responsibilities: Optional[str] = Field(
        default=None,
        max_length=400,
        description="Specific responsibilities in the meeting/discussion",
    )
    expertise_keywords: List[str] = Field(
        default_factory=list,
        max_length=20,
        description="Keywords representing areas of technical competence (e.g. ['PostgreSQL', 'Kafka'])",
    )
    session_goals: Optional[str] = Field(
        default=None,
        max_length=400,
        description="Goals or intended contributions for this session",
    )
    constraints: Optional[str] = Field(
        default=None,
        max_length=400,
        description="Topics, topics to avoid, or constraints on contribution",
    )

    @model_validator(mode="before")
    @classmethod
    def strip_whitespace(cls, values: dict) -> dict:
        """Strip leading/trailing whitespace from string fields."""
        if not isinstance(values, dict):
            return values
        cleaned = {}
        for k, v in values.items():
            if isinstance(v, str):
                v_clean = v.strip()
                cleaned[k] = v_clean if v_clean else None
            elif isinstance(v, list) and k == "expertise_keywords":
                cleaned[k] = [item.strip() for item in v if isinstance(item, str) and item.strip()]
            else:
                cleaned[k] = v
        return cleaned

    @model_validator(mode="after")
    def validate_cumulative_length(self) -> "ParticipantContext":
        """Enforce strict cumulative 1,500 character limit across all fields."""
        total = 0
        if self.preferred_name:
            total += len(self.preferred_name)
        if self.role:
            total += len(self.role)
        if self.responsibilities:
            total += len(self.responsibilities)
        if self.session_goals:
            total += len(self.session_goals)
        if self.constraints:
            total += len(self.constraints)
        if self.expertise_keywords:
            total += sum(len(kw) for kw in self.expertise_keywords)

        if total > 1500:
            raise ValueError(
                f"Cumulative participant context length ({total} chars) exceeds the maximum allowed limit of 1500 characters."
            )
        return self


class UpdateParticipantContextRequest(ParticipantContext):
    """Payload for updating or setting participant context on an active session."""
    pass
