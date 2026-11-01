from typing import Any, TypedDict


class AgentState(TypedDict, total=False):

    # Session information
    session_id: str
    situation: str
    objective: str
    communication_goals: list[str]
    response_preferences: dict[str, Any]
    constraints: list[str]
    participant_context: dict[str, Any] | None

    # Conversation input
    messages: list[dict[str, Any]]

    # Conversation understanding
    analysis: dict[str, Any]
    current_topic: str
    conversation_summary: str
    key_points: list[str]
    unresolved_questions: list[str]

    # Intent analysis
    detected_intent: str
    intent_evidence: str

    # Goal relevance
    relevance: dict[str, Any]
    should_suggest: bool

    # Response planning
    response_plan: dict[str, Any]

    # Generation
    suggestions: list[dict[str, Any]]
    previous_suggestions: list[dict[str, Any]]

    # Validation & Status
    validation_warnings: list[str]
    status: str

    # Execution telemetry
    selected_model: str
    stage_reached: str
    error_stage: str
    error_category: str
    error_message: str
