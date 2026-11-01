"""Evaluation schemas for Conversa AI.

Defines Pydantic models for evaluation dataset scenarios, execution results,
relevance metrics, generation metrics, infrastructure metrics, usefulness review,
factual accuracy review, and overall evaluation runs.
"""

from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class ResponsePreferences(BaseModel):
    """Preferences for agent suggestion generation."""

    style: Literal["natural", "professional", "casual", "technical"] = "natural"
    length: Literal["short", "concise", "detailed"] = "concise"
    technical_level: Literal["beginner", "intermediate", "advanced"] = "intermediate"


class Scenario(BaseModel):
    """A realistic conversation scenario for evaluating Conversa AI."""

    scenario_id: str = Field(..., description="Unique scenario identifier")
    scenario_category: Literal["positive", "negative"] = Field(
        ..., description="Whether agent should identify an opportunity (positive) or abstain (negative)"
    )
    scenario_type: str = Field(
        ..., description="Specific evaluation subtype (e.g., statement_no_question, superficial_keyword)"
    )
    situation: str = Field(..., min_length=5, max_length=3000, description="User's context/setting")
    objective: str = Field(..., min_length=5, max_length=3000, description="User's high-level goal")
    communication_goals: list[str] = Field(
        default_factory=list, max_length=10, description="Specific goals the user aims to address"
    )
    response_preferences: ResponsePreferences = Field(
        default_factory=ResponsePreferences, description="Tone, length, and technical preferences"
    )
    constraints: list[str] = Field(
        default_factory=list, max_length=20, description="Hard constraints on proposed suggestions"
    )
    messages: list[str] = Field(
        ..., min_length=1, description="Chronological conversation utterances without speaker labels"
    )
    expected_relevance: bool = Field(
        ..., description="Expected relevance decision (True for suggestions_ready, False for no_suggestion)"
    )
    expected_contribution_opportunity: str = Field(
        ..., description="Description of the contribution opportunity or why none exists"
    )
    expected_response_characteristics: dict[str, Any] = Field(
        default_factory=dict, description="Expected characteristics of generated responses"
    )
    reference_facts: list[str] = Field(
        default_factory=list, description="Ground-truth facts against which claims can be verified"
    )
    expected_decision_explanation: str = Field(
        ..., description="Explanation of why this decision is expected"
    )


class ScenarioExecution(BaseModel):
    """The outcome of executing a single scenario against the Conversa AI API."""

    scenario_id: str
    scenario_category: Literal["positive", "negative"]
    scenario_type: str
    expected_relevance: bool
    execution_status: Literal["success", "failed", "skipped"]
    http_status_code: int | None = None
    session_id: str | None = None
    actual_relevance: bool | None = None
    relevance_decision_evaluated: bool = False
    relevance_confidence: float | None = None
    relevance_reason: str | None = None
    response_opportunity: str | None = None
    detected_intent: str | None = None
    current_topic: str | None = None
    conversation_summary: str | None = None
    understanding_result: dict[str, Any] | None = None
    relevance_result: dict[str, Any] | None = None
    suggestions: list[dict[str, Any]] = Field(default_factory=list)
    generated_suggestion_count: int = 0
    validation_warnings: list[str] = Field(default_factory=list)
    classification: Literal["TP", "FP", "TN", "FN", "ERROR", "SKIPPED"]
    failure_stage: str | None = None
    exception_category: str | None = None
    error_message: str | None = None
    error_detail: str | None = None
    selected_model: str | None = None
    latency_ms: float = 0.0
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    raw_response: dict[str, Any] | None = None


class ConfusionMatrix(BaseModel):
    """Relevance classification matrix."""

    tp: int = Field(default=0, description="True Positives: Correctly identified opportunities")
    fp: int = Field(default=0, description="False Positives: Suggested when should have abstained")
    tn: int = Field(default=0, description="True Negatives: Correctly abstained")
    fn: int = Field(default=0, description="False Negatives: Missed valid opportunity")
    errors: int = Field(default=0, description="API or execution errors (unclassified)")


class RelevanceMetrics(BaseModel):
    """Metrics evaluated strictly from successfully evaluated relevance decisions."""

    total_relevance_evaluations: int = 0
    tp: int = 0
    fp: int = 0
    tn: int = 0
    fn: int = 0
    precision: float | None = None
    recall: float | None = None
    f1_score: float | None = None
    false_positive_rate: float | None = None
    missed_opportunity_rate: float | None = None


class GenerationMetrics(BaseModel):
    """Metrics tracking end-to-end suggestion generation performance."""

    positive_scenarios_evaluated: int = 0
    positive_scenarios_with_suggestions: int = 0
    positive_scenarios_relevant_but_no_suggestion: int = 0
    suggestions_rejected_by_validation: int = 0
    generation_failure_rate: float | None = None
    avg_suggestions_per_successful_positive: float | None = None
    total_suggestions_generated: int = 0


class InfrastructureMetrics(BaseModel):
    """Metrics tracking system execution reliability and stage failures."""

    total_scenarios: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    successful_execution_rate: float = 0.0
    failed_execution_rate: float = 0.0
    failure_rate_by_stage: dict[str, int] = Field(default_factory=dict)
    failure_categories: dict[str, int] = Field(default_factory=dict)


class UsefulnessCriteriaBreakdown(BaseModel):
    """Breakdown of satisfaction for individual usefulness criteria."""

    conversation_relevance: float = 0.0
    objective_alignment: float = 0.0
    communication_goal_alignment: float = 0.0
    practical_value: float = 0.0
    natural_conversational_language: float = 0.0
    conciseness: float = 0.0
    non_repetition: float = 0.0


class UsefulnessSummary(BaseModel):
    """Summary of suggestion usefulness ratings."""

    total_suggestions_evaluated: int = 0
    average_score: float | None = None
    score_distribution: dict[int, int] = Field(
        default_factory=lambda: {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    )
    criteria_compliance: UsefulnessCriteriaBreakdown = Field(
        default_factory=UsefulnessCriteriaBreakdown
    )
    pending_human_review: bool = True


class FactualitySummary(BaseModel):
    """Summary of claim factuality ratings."""

    total_claims: int = 0
    supported_claims: int = 0
    unsupported_claims: int = 0
    incorrect_claims: int = 0
    not_applicable_claims: int = 0
    total_verifiable_claims: int = 0
    factual_accuracy: float | None = None
    pending_human_review: bool = True


class MetricsSummary(BaseModel):
    """Comprehensive separated evaluation metrics."""

    total_scenarios: int
    successful_executions: int
    failed_executions: int

    # Separated metric dimensions
    relevance: RelevanceMetrics = Field(default_factory=RelevanceMetrics)
    generation: GenerationMetrics = Field(default_factory=GenerationMetrics)
    infrastructure: InfrastructureMetrics = Field(default_factory=InfrastructureMetrics)
    usefulness: UsefulnessSummary = Field(default_factory=UsefulnessSummary)
    factuality: FactualitySummary = Field(default_factory=FactualitySummary)

    # Legacy/compatibility fields
    confusion_matrix: ConfusionMatrix = Field(default_factory=ConfusionMatrix)
    precision: float | None = None
    recall: float | None = None
    missed_opportunity_rate: float | None = None
    f1_score: float | None = None
    positive_total: int = 0
    positive_successful: int = 0
    positive_failed: int = 0
    negative_total: int = 0
    negative_successful: int = 0
    negative_failed: int = 0


class UsefulnessReviewItem(BaseModel):
    """Individual human review record for a generated suggestion."""

    review_id: str = Field(default_factory=lambda: str(uuid4()))
    scenario_id: str
    suggestion_index: int
    suggestion_text: str
    response_type: str
    rationale: str
    score: int | None = Field(
        default=None,
        ge=1,
        le=5,
        description="1=Completely Irrelevant, 2=Weak, 3=Partially Useful, 4=Useful/Relevant, 5=Highly Useful"
    )
    relevance_to_conversation: bool | None = None
    alignment_with_objective: bool | None = None
    alignment_with_communication_goals: bool | None = None
    practical_value: bool | None = None
    natural_conversational_language: bool | None = None
    conciseness: bool | None = None
    non_repetition: bool | None = None
    notes: str | None = None
    reviewer: str | None = None
    reviewed_at: str | None = None
    is_secondary_llm: bool = False


class FactualityReviewItem(BaseModel):
    """Individual claim review record for factual accuracy evaluation."""

    review_id: str = Field(default_factory=lambda: str(uuid4()))
    scenario_id: str
    suggestion_index: int
    claim_index: int
    claim_text: str
    classification: Literal["supported", "unsupported", "incorrect", "not_applicable"] | None = None
    explanation: str | None = None
    reference_fact_matched: str | None = None
    reviewer: str | None = None
    reviewed_at: str | None = None


class EvaluationRun(BaseModel):
    """Complete evaluation run container."""

    run_id: str
    timestamp: str
    base_url: str
    model_configured: str
    model_resolved: str | None = None
    total_scenarios: int
    executions: list[ScenarioExecution]
    metrics: MetricsSummary
