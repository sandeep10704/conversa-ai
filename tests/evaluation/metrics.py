"""Evaluation metric calculations for Conversa AI.

Implements cleanly separated metric dimensions:
1. Relevance Classification Metrics (precision, recall, F1, FPR, missed opportunity rate)
2. End-to-End Suggestion Generation Metrics (suggestion yield, rejection rate, failure rate)
3. Infrastructure & Execution Reliability Metrics (success rate, stage failure breakdown)
4. Qualitative Human Review Summaries (Usefulness and Factuality)
"""

from typing import Sequence

from tests.evaluation.schemas import (
    ConfusionMatrix,
    FactualityReviewItem,
    FactualitySummary,
    GenerationMetrics,
    InfrastructureMetrics,
    MetricsSummary,
    RelevanceMetrics,
    ScenarioExecution,
    UsefulnessCriteriaBreakdown,
    UsefulnessReviewItem,
    UsefulnessSummary,
)


def compute_relevance_metrics(executions: Sequence[ScenarioExecution]) -> RelevanceMetrics:
    """Compute relevance classification metrics strictly from successfully evaluated decisions.

    Ignores infrastructure errors that failed before relevance was evaluated.
    """
    tp = 0
    fp = 0
    tn = 0
    fn = 0

    for ex in executions:
        # Only evaluate scenarios where relevance decision was actually determined
        if ex.execution_status == "failed" and not ex.relevance_decision_evaluated:
            continue

        if ex.actual_relevance is None:
            continue

        if ex.expected_relevance:
            if ex.actual_relevance is True:
                tp += 1
            else:
                fn += 1
        else:
            if ex.actual_relevance is False:
                tn += 1
            else:
                fp += 1

    total = tp + fp + tn + fn
    precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 0.0
    recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 0.0
    f1 = round(2 * (precision * recall) / (precision + recall), 4) if (precision + recall) > 0 else 0.0
    fpr = round(fp / (fp + tn), 4) if (fp + tn) > 0 else 0.0
    missed_opp = round(fn / (tp + fn), 4) if (tp + fn) > 0 else 0.0

    return RelevanceMetrics(
        total_relevance_evaluations=total,
        tp=tp,
        fp=fp,
        tn=tn,
        fn=fn,
        precision=precision,
        recall=recall,
        f1_score=f1,
        false_positive_rate=fpr,
        missed_opportunity_rate=missed_opp,
    )


def compute_generation_metrics(executions: Sequence[ScenarioExecution]) -> GenerationMetrics:
    """Compute suggestion generation yield and failure rates for positive scenarios."""
    positive_execs = [e for e in executions if e.expected_relevance and e.execution_status == "success"]

    pos_evaluated = len(positive_execs)
    pos_with_sug = sum(1 for e in positive_execs if len(e.suggestions) > 0)
    pos_rel_true = sum(1 for e in positive_execs if e.actual_relevance is True)
    pos_rel_true_no_sug = sum(
        1 for e in positive_execs if e.actual_relevance is True and len(e.suggestions) == 0
    )

    total_rejected = sum(len(e.validation_warnings) for e in positive_execs)
    total_suggestions = sum(len(e.suggestions) for e in positive_execs)

    gen_failure_rate = (
        round(pos_rel_true_no_sug / pos_rel_true, 4) if pos_rel_true > 0 else 0.0
    )
    avg_sug = (
        round(total_suggestions / pos_with_sug, 2) if pos_with_sug > 0 else 0.0
    )

    return GenerationMetrics(
        positive_scenarios_evaluated=pos_evaluated,
        positive_scenarios_with_suggestions=pos_with_sug,
        positive_scenarios_relevant_but_no_suggestion=pos_rel_true_no_sug,
        suggestions_rejected_by_validation=total_rejected,
        generation_failure_rate=gen_failure_rate,
        avg_suggestions_per_successful_positive=avg_sug,
        total_suggestions_generated=total_suggestions,
    )


def compute_infrastructure_metrics(executions: Sequence[ScenarioExecution]) -> InfrastructureMetrics:
    """Compute execution reliability, stage-level failure counts, and error categories."""
    total = len(executions)
    successful = sum(1 for e in executions if e.execution_status == "success")
    failed = sum(1 for e in executions if e.execution_status == "failed")

    succ_rate = round(successful / total, 4) if total > 0 else 0.0
    fail_rate = round(failed / total, 4) if total > 0 else 0.0

    stage_breakdown: dict[str, int] = {}
    category_breakdown: dict[str, int] = {}

    for e in executions:
        if e.execution_status == "failed":
            st = e.failure_stage or "unknown"
            cat = e.exception_category or "unknown_error"
            stage_breakdown[st] = stage_breakdown.get(st, 0) + 1
            category_breakdown[cat] = category_breakdown.get(cat, 0) + 1

    return InfrastructureMetrics(
        total_scenarios=total,
        successful_executions=successful,
        failed_executions=failed,
        successful_execution_rate=succ_rate,
        failed_execution_rate=fail_rate,
        failure_rate_by_stage=stage_breakdown,
        failure_categories=category_breakdown,
    )


def compute_confusion_matrix(executions: Sequence[ScenarioExecution]) -> ConfusionMatrix:
    """Compute legacy confusion matrix."""
    rel = compute_relevance_metrics(executions)
    errors = sum(1 for e in executions if e.execution_status == "failed")
    return ConfusionMatrix(
        tp=rel.tp,
        fp=rel.fp,
        tn=rel.tn,
        fn=rel.fn,
        errors=errors,
    )


def compute_usefulness_summary(
    review_items: Sequence[UsefulnessReviewItem] | None,
) -> UsefulnessSummary:
    """Compute aggregate usefulness statistics from human review items."""
    if not review_items:
        return UsefulnessSummary(
            total_suggestions_evaluated=0,
            average_score=None,
            score_distribution={1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
            criteria_compliance=UsefulnessCriteriaBreakdown(),
            pending_human_review=True,
        )

    scored_items = [item for item in review_items if item.score is not None]
    if not scored_items:
        return UsefulnessSummary(
            total_suggestions_evaluated=len(review_items),
            average_score=None,
            score_distribution={1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
            criteria_compliance=UsefulnessCriteriaBreakdown(),
            pending_human_review=True,
        )

    distribution: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    total_score = 0
    for item in scored_items:
        score = item.score
        if score in distribution:
            distribution[score] += 1
        total_score += score

    avg_score = round(total_score / len(scored_items), 2)
    n = len(scored_items)

    def pct(field_name: str) -> float:
        matches = sum(1 for item in scored_items if getattr(item, field_name) is True)
        return round((matches / n) * 100, 1)

    compliance = UsefulnessCriteriaBreakdown(
        conversation_relevance=pct("relevance_to_conversation"),
        objective_alignment=pct("alignment_with_objective"),
        communication_goal_alignment=pct("alignment_with_communication_goals"),
        practical_value=pct("practical_value"),
        natural_conversational_language=pct("natural_conversational_language"),
        conciseness=pct("conciseness"),
        non_repetition=pct("non_repetition"),
    )

    return UsefulnessSummary(
        total_suggestions_evaluated=len(scored_items),
        average_score=avg_score,
        score_distribution=distribution,
        criteria_compliance=compliance,
        pending_human_review=False,
    )


def compute_factuality_summary(
    fact_items: Sequence[FactualityReviewItem] | None,
) -> FactualitySummary:
    """Compute claim-level factual accuracy from review items."""
    if not fact_items:
        return FactualitySummary(pending_human_review=True)

    classified_items = [
        item for item in fact_items if item.classification is not None
    ]
    if not classified_items:
        return FactualitySummary(
            total_claims=len(fact_items),
            pending_human_review=True,
        )

    supported = sum(1 for i in classified_items if i.classification == "supported")
    unsupported = sum(1 for i in classified_items if i.classification == "unsupported")
    incorrect = sum(1 for i in classified_items if i.classification == "incorrect")
    na = sum(1 for i in classified_items if i.classification == "not_applicable")

    verifiable = supported + unsupported + incorrect
    accuracy = round(supported / verifiable, 4) if verifiable > 0 else None

    return FactualitySummary(
        total_claims=len(classified_items),
        supported_claims=supported,
        unsupported_claims=unsupported,
        incorrect_claims=incorrect,
        not_applicable_claims=na,
        total_verifiable_claims=verifiable,
        factual_accuracy=accuracy,
        pending_human_review=False,
    )


def calculate_metrics(
    executions: Sequence[ScenarioExecution],
    usefulness_reviews: Sequence[UsefulnessReviewItem] | None = None,
    factuality_reviews: Sequence[FactualityReviewItem] | None = None,
) -> MetricsSummary:
    """Calculate comprehensive separated evaluation metrics across executions and reviews."""
    total = len(executions)
    successful = sum(1 for e in executions if e.execution_status == "success")
    failed = sum(1 for e in executions if e.execution_status == "failed")

    pos_items = [e for e in executions if e.expected_relevance]
    neg_items = [e for e in executions if not e.expected_relevance]

    rel_metrics = compute_relevance_metrics(executions)
    gen_metrics = compute_generation_metrics(executions)
    infra_metrics = compute_infrastructure_metrics(executions)
    cm = compute_confusion_matrix(executions)

    usefulness_summary = compute_usefulness_summary(usefulness_reviews)
    factuality_summary = compute_factuality_summary(factuality_reviews)

    return MetricsSummary(
        total_scenarios=total,
        successful_executions=successful,
        failed_executions=failed,
        relevance=rel_metrics,
        generation=gen_metrics,
        infrastructure=infra_metrics,
        usefulness=usefulness_summary,
        factuality=factuality_summary,
        confusion_matrix=cm,
        precision=rel_metrics.precision,
        recall=rel_metrics.recall,
        missed_opportunity_rate=rel_metrics.missed_opportunity_rate,
        f1_score=rel_metrics.f1_score,
        positive_total=len(pos_items),
        positive_successful=sum(1 for e in pos_items if e.execution_status == "success"),
        positive_failed=sum(1 for e in pos_items if e.execution_status == "failed"),
        negative_total=len(neg_items),
        negative_successful=sum(1 for e in neg_items if e.execution_status == "success"),
        negative_failed=sum(1 for e in neg_items if e.execution_status == "failed"),
    )
