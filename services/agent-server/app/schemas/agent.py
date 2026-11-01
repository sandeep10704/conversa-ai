from typing import Literal

from pydantic import BaseModel, Field


class DiscussionAnalysis(BaseModel):

    current_topic: str

    summary: str

    key_points: list[str] = Field(default_factory=list)

    unresolved_questions: list[str] = Field(
        default_factory=list
    )

    detected_intent: Literal[
        "question",
        "opinion_request",
        "disagreement",
        "request_for_explanation",
        "statement",
        "topic_shift",
        "unclear"
    ] = "unclear"

    intent_evidence: str = ""


class RelevanceAssessment(BaseModel):

    should_suggest: bool

    confidence: float = Field(
        ge=0,
        le=1
    )

    reason: str

    response_opportunity: str


class SuggestionItem(BaseModel):

    response_type: Literal[
        "direct_answer",
        "technical_explanation",
        "supporting_point",
        "polite_disagreement",
        "example",
        "follow_up_question",
        "perspective"
    ]

    text: str

    rationale: str

    follow_up_question: str | None = None


class SuggestionSet(BaseModel):

    suggestions: list[SuggestionItem] = Field(
        default_factory=list,
        max_length=2
    )


class AgentRunResponse(BaseModel):

    session_id: str

    status: Literal[
        "suggestions_ready",
        "no_suggestion"
    ]

    current_topic: str

    conversation_summary: str

    detected_intent: str

    relevance_confidence: float

    relevance_reason: str

    suggestions: list[SuggestionItem]
    validation_warnings: list[str] = []

    selected_model: str | None = None
    stage_reached: str | None = None
    should_suggest: bool | None = None
    response_opportunity: str | None = None
    failure_stage: str | None = None
    exception_category: str | None = None
