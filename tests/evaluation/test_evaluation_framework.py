"""Unit and integration tests for Conversa AI evaluation framework.

Verifies schema integrity, dataset loading, confusion matrix calculation,
metric formulas, review management, and report generation.
"""

from pathlib import Path
import pytest

from tests.evaluation.dataset_loader import load_dataset, validate_dataset_integrity
from tests.evaluation.factuality_review import (
    create_factuality_review_template,
    extract_candidate_claims,
)
from tests.evaluation.metrics import (
    calculate_metrics,
    compute_confusion_matrix,
    compute_factuality_summary,
    compute_usefulness_summary,
)
from tests.evaluation.report_generator import generate_markdown_report
from tests.evaluation.schemas import (
    EvaluationRun,
    FactualityReviewItem,
    MetricsSummary,
    Scenario,
    ScenarioExecution,
    UsefulnessReviewItem,
)
from tests.evaluation.usefulness_review import create_usefulness_review_template


class TestDatasetIntegrity:
    """Validate 24-scenario evaluation dataset constraints."""

    def test_dataset_loads_all_24_scenarios(self):
        scenarios = load_dataset()
        assert len(scenarios) == 24, f"Expected 24 scenarios, found {len(scenarios)}"

    def test_dataset_balanced_positive_and_negative(self):
        scenarios = load_dataset()
        positive = [s for s in scenarios if s.scenario_category == "positive"]
        negative = [s for s in scenarios if s.scenario_category == "negative"]
        assert len(positive) == 12, f"Expected 12 positive scenarios, found {len(positive)}"
        assert len(negative) == 12, f"Expected 12 negative scenarios, found {len(negative)}"

        for s in positive:
            assert s.expected_relevance is True
        for s in negative:
            assert s.expected_relevance is False

    def test_unique_scenario_ids(self):
        scenarios = load_dataset()
        ids = [s.scenario_id for s in scenarios]
        assert len(ids) == len(set(ids)), "Duplicate scenario IDs detected"

    def test_no_speaker_identities_or_diarization(self):
        scenarios = load_dataset()
        integrity = validate_dataset_integrity(scenarios)
        assert integrity["is_balanced"] is True
        assert len(integrity["speaker_leak_warnings"]) == 0, (
            f"Speaker leaks found: {integrity['speaker_leak_warnings']}"
        )

    def test_scenario_filtering(self):
        pos_scenarios = load_dataset(category="positive")
        assert len(pos_scenarios) == 12
        assert all(s.scenario_category == "positive" for s in pos_scenarios)

        filtered = load_dataset(scenario_ids=["pos_01_statement_no_question"])
        assert len(filtered) == 1
        assert filtered[0].scenario_id == "pos_01_statement_no_question"


class TestMetricsCalculation:
    """Validate mathematical correctness of evaluation metrics."""

    def test_confusion_matrix_and_metrics_calculation(self):
        executions = [
            ScenarioExecution(
                scenario_id="p1", scenario_category="positive", scenario_type="t",
                expected_relevance=True, execution_status="success", actual_relevance=True,
                classification="TP"
            ),
            ScenarioExecution(
                scenario_id="p2", scenario_category="positive", scenario_type="t",
                expected_relevance=True, execution_status="success", actual_relevance=True,
                classification="TP"
            ),
            ScenarioExecution(
                scenario_id="p3", scenario_category="positive", scenario_type="t",
                expected_relevance=True, execution_status="success", actual_relevance=False,
                classification="FN"
            ),
            ScenarioExecution(
                scenario_id="n1", scenario_category="negative", scenario_type="t",
                expected_relevance=False, execution_status="success", actual_relevance=False,
                classification="TN"
            ),
            ScenarioExecution(
                scenario_id="n2", scenario_category="negative", scenario_type="t",
                expected_relevance=False, execution_status="success", actual_relevance=True,
                classification="FP"
            ),
            ScenarioExecution(
                scenario_id="p4", scenario_category="positive", scenario_type="t",
                expected_relevance=True, execution_status="failed", actual_relevance=None,
                classification="ERROR"
            ),
        ]

        cm = compute_confusion_matrix(executions)
        assert cm.tp == 2
        assert cm.fn == 1
        assert cm.tn == 1
        assert cm.fp == 1
        assert cm.errors == 1

        metrics = calculate_metrics(executions)
        # Precision = TP / (TP + FP) = 2 / (2 + 1) = 0.6667
        assert metrics.precision == pytest.approx(0.6667, abs=1e-4)
        # Recall = TP / (TP + FN) = 2 / (2 + 1) = 0.6667
        assert metrics.recall == pytest.approx(0.6667, abs=1e-4)
        # Missed Opportunity Rate = FN / (TP + FN) = 1 / (2 + 1) = 0.3333
        assert metrics.missed_opportunity_rate == pytest.approx(0.3333, abs=1e-4)
        # F1 = 2 * (P * R) / (P + R) = 0.6667
        assert metrics.f1_score == pytest.approx(0.6667, abs=1e-4)
        assert metrics.successful_executions == 5
        assert metrics.failed_executions == 1

    def test_metrics_zero_denominators(self):
        executions = [
            ScenarioExecution(
                scenario_id="n1", scenario_category="negative", scenario_type="t",
                expected_relevance=False, execution_status="success", actual_relevance=False,
                classification="TN"
            )
        ]
        metrics = calculate_metrics(executions)
        assert metrics.precision == 0.0
        assert metrics.recall == 0.0
        assert metrics.missed_opportunity_rate == 0.0
        assert metrics.f1_score == 0.0


class TestHumanReviewProcessing:
    """Validate usefulness and factuality review processing."""

    def test_usefulness_summary_calculation(self):
        reviews = [
            UsefulnessReviewItem(
                scenario_id="p1", suggestion_index=0, suggestion_text="Fix pool.",
                response_type="technical_explanation", rationale="Clear root cause.",
                score=4, relevance_to_conversation=True, alignment_with_objective=True,
                alignment_with_communication_goals=True, practical_value=True,
                natural_conversational_language=True, conciseness=True, non_repetition=True,
            ),
            UsefulnessReviewItem(
                scenario_id="p2", suggestion_index=0, suggestion_text="Use outbox.",
                response_type="technical_explanation", rationale="Solves dual write.",
                score=5, relevance_to_conversation=True, alignment_with_objective=True,
                alignment_with_communication_goals=True, practical_value=True,
                natural_conversational_language=True, conciseness=True, non_repetition=True,
            ),
        ]
        summary = compute_usefulness_summary(reviews)
        assert summary.pending_human_review is False
        assert summary.total_suggestions_evaluated == 2
        assert summary.average_score == 4.5
        assert summary.score_distribution[4] == 1
        assert summary.score_distribution[5] == 1
        assert summary.criteria_compliance.conversation_relevance == 100.0

    def test_factuality_summary_calculation(self):
        items = [
            FactualityReviewItem(
                scenario_id="p1", suggestion_index=0, claim_index=0,
                claim_text="Auth-proxy pool exhausted.", classification="supported"
            ),
            FactualityReviewItem(
                scenario_id="p1", suggestion_index=0, claim_index=1,
                claim_text="Database pool was 1000.", classification="unsupported"
            ),
            FactualityReviewItem(
                scenario_id="p2", suggestion_index=0, claim_index=0,
                claim_text="Kafka dropped 50% packets.", classification="incorrect"
            ),
            FactualityReviewItem(
                scenario_id="p2", suggestion_index=0, claim_index=1,
                claim_text="Hello team.", classification="not_applicable"
            ),
        ]
        summary = compute_factuality_summary(items)
        assert summary.pending_human_review is False
        assert summary.supported_claims == 1
        assert summary.unsupported_claims == 1
        assert summary.incorrect_claims == 1
        assert summary.not_applicable_claims == 1
        assert summary.total_verifiable_claims == 3
        # Factual Accuracy = 1 / 3 = 0.3333
        assert summary.factual_accuracy == pytest.approx(0.3333, abs=1e-4)

    def test_claim_extraction(self):
        text = "Auth-proxy pool was exhausted. S3 standard costs $1,800/month. What should we do?"
        claims = extract_candidate_claims(text)
        assert len(claims) >= 2


class TestReportGenerator:
    """Validate Markdown evaluation report generation."""

    def test_markdown_report_generation(self):
        run = EvaluationRun(
            run_id="test_run",
            timestamp="2026-10-31T12:00:00Z",
            base_url="http://localhost:8001",
            model_configured="openrouter/free",
            total_scenarios=2,
            executions=[
                ScenarioExecution(
                    scenario_id="pos_01_statement_no_question",
                    scenario_category="positive",
                    scenario_type="statement_without_question",
                    expected_relevance=True,
                    execution_status="success",
                    actual_relevance=True,
                    relevance_confidence=0.9,
                    relevance_reason="Matches auth-proxy topic",
                    detected_intent="statement",
                    suggestions=[{"response_type": "technical_explanation", "text": "Check connection pool", "rationale": "Direct cause"}],
                    classification="TP",
                ),
                ScenarioExecution(
                    scenario_id="neg_01_completely_unrelated",
                    scenario_category="negative",
                    scenario_type="completely_unrelated",
                    expected_relevance=False,
                    execution_status="success",
                    actual_relevance=False,
                    relevance_confidence=0.1,
                    relevance_reason="Off-topic food talk",
                    detected_intent="statement",
                    suggestions=[],
                    classification="TN",
                ),
            ],
            metrics=calculate_metrics([
                ScenarioExecution(
                    scenario_id="pos_01_statement_no_question",
                    scenario_category="positive",
                    scenario_type="statement_without_question",
                    expected_relevance=True,
                    execution_status="success",
                    actual_relevance=True,
                    classification="TP",
                ),
                ScenarioExecution(
                    scenario_id="neg_01_completely_unrelated",
                    scenario_category="negative",
                    scenario_type="completely_unrelated",
                    expected_relevance=False,
                    execution_status="success",
                    actual_relevance=False,
                    classification="TN",
                ),
            ]),
        )
        report_md = generate_markdown_report(run)
        assert "# Conversa AI Evaluation Report" in report_md
        assert "## 1. Executive Summary" in report_md
        assert "## 2. Relevance Classification Performance" in report_md
        assert "## 3. End-to-End Suggestion Generation Performance" in report_md
        assert "## 4. Stage-Specific Failure Analysis" in report_md
        assert "## 8. Comparison with Previous Benchmark" in report_md
        assert "## 9. Recommendations for Engineering Roadmap" in report_md
