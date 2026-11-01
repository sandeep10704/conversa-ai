"""Evaluation runner for Conversa AI.

Executes scenarios against the running Conversa AI agent server via REST APIs:
1. Creates session (POST /v1/sessions)
2. Posts chronological messages (POST /v1/sessions/{session_id}/messages)
3. Triggers suggestion generation (POST /v1/sessions/{session_id}/suggest)
4. Captures decisions, confidence, suggestions, latencies, stages, and errors
5. Calculates separated relevance, generation, and infrastructure metrics
6. Emits structured review files for usefulness and factuality
7. Generates comprehensive Markdown evaluation report
"""

import argparse
import asyncio
from datetime import datetime
import json
import logging
from pathlib import Path
import time
from typing import Any

import httpx

from tests.evaluation.dataset_loader import load_dataset
from tests.evaluation.factuality_review import (
    create_factuality_review_template,
    save_factuality_reviews,
)
from tests.evaluation.metrics import calculate_metrics
from tests.evaluation.report_generator import generate_report_file
from tests.evaluation.schemas import (
    EvaluationRun,
    Scenario,
    ScenarioExecution,
)
from tests.evaluation.usefulness_review import (
    create_usefulness_review_template,
    save_usefulness_reviews,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("evaluation_runner")

DEFAULT_BASE_URL = "http://localhost:8001"
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent / "results"
DEFAULT_REPORTS_DIR = Path(__file__).resolve().parent / "reports"


def classify_error_details(
    status_code: int | None,
    resp_text: str | None,
    exc: Exception | None,
) -> tuple[str, str, str]:
    """Determine (failure_stage, exception_category, error_summary)."""
    text = (resp_text or "").lower()
    exc_str = (str(exc) or "").lower()
    combined = f"{text} {exc_str}"

    stage = "unknown"
    category = "unknown_error"

    # Attempt to parse structured error JSON from API
    if resp_text:
        try:
            parsed = json.loads(resp_text)
            detail = parsed.get("detail")
            if isinstance(detail, dict):
                stage = detail.get("stage", stage)
                category = detail.get("category", category)
                msg = detail.get("message") or detail.get("error") or resp_text
                return stage, category, str(msg)
        except Exception:
            pass

    # Infer stage
    if "understanding" in combined:
        stage = "understanding"
    elif "relevance" in combined:
        stage = "relevance"
    elif "generator" in combined or "suggestionset" in combined:
        stage = "generator"
    elif "validator" in combined:
        stage = "validator"
    elif "messages" in combined or "sessions" in combined:
        stage = "session_setup"
    elif exc is not None and isinstance(exc, (httpx.TimeoutException, httpx.RequestError)):
        stage = "network"

    # Infer category
    if "rate limit" in combined or "429" in combined or "toomanyrequests" in combined:
        category = "rate_limit"
    elif "timeout" in combined or isinstance(exc, httpx.TimeoutException):
        category = "timeout"
    elif "json" in combined or "jsondecodeerror" in combined or "parse" in combined:
        category = "json_parse_error"
    elif "validation" in combined or "valueerror" in combined or "pydantic" in combined:
        category = "validation_error"
    elif isinstance(exc, httpx.RequestError):
        category = "network_error"
    elif status_code is not None:
        category = f"http_{status_code}"

    summary = resp_text or str(exc) or "Unknown error"
    return stage, category, summary[:300]


async def execute_scenario(
    client: httpx.AsyncClient,
    base_url: str,
    scenario: Scenario,
    retries: int = 3,
    backoff_factor: float = 2.0,
) -> ScenarioExecution:
    """Execute a single scenario end-to-end against the Conversa AI API."""
    logger.info(f"Executing scenario: {scenario.scenario_id} [{scenario.scenario_category}]")
    start_time = time.perf_counter()

    last_error: Exception | None = None
    last_status_code: int | None = None
    last_error_text: str | None = None
    last_stage = "unknown"
    last_category = "unknown_error"

    for attempt in range(1, retries + 1):
        try:
            # 1. Create Session
            session_payload = {
                "situation": scenario.situation,
                "objective": scenario.objective,
                "communication_goals": scenario.communication_goals,
                "response_preferences": scenario.response_preferences.model_dump(),
                "constraints": scenario.constraints,
            }

            resp = await client.post(
                f"{base_url}/v1/sessions",
                json=session_payload,
                timeout=30.0,
            )
            if resp.status_code != 201:
                last_status_code = resp.status_code
                last_error_text = resp.text
                last_stage, last_category, _ = classify_error_details(
                    resp.status_code, resp.text, None
                )
                logger.warning(
                    f"[{scenario.scenario_id}] Attempt {attempt} session create failed: {resp.status_code}"
                )
                await asyncio.sleep(backoff_factor * attempt)
                continue

            session_id = resp.json().get("session_id")
            if not session_id:
                raise RuntimeError(f"No session_id in response: {resp.text}")

            # 2. Add chronological messages
            msg_failed = False
            for msg_idx, message_text in enumerate(scenario.messages):
                msg_resp = await client.post(
                    f"{base_url}/v1/sessions/{session_id}/messages",
                    json={"text": message_text, "source": "manual"},
                    timeout=30.0,
                )
                if msg_resp.status_code != 201:
                    last_status_code = msg_resp.status_code
                    last_error_text = msg_resp.text
                    last_stage, last_category, _ = classify_error_details(
                        msg_resp.status_code, msg_resp.text, None
                    )
                    msg_failed = True
                    break

            if msg_failed:
                await asyncio.sleep(backoff_factor * attempt)
                continue

            # 3. Call /suggest
            suggest_resp = await client.post(
                f"{base_url}/v1/sessions/{session_id}/suggest",
                headers={"Content-Type": "application/json"},
                timeout=120.0,
            )

            latency_ms = round((time.perf_counter() - start_time) * 1000, 2)

            if suggest_resp.status_code == 200:
                raw_data = suggest_resp.json()
                status_str = raw_data.get("status", "no_suggestion")
                should_suggest_val = raw_data.get("should_suggest")
                if should_suggest_val is not None:
                    actual_relevance = bool(should_suggest_val)
                else:
                    actual_relevance = (
                        status_str == "suggestions_ready"
                        or raw_data.get("stage_reached") in ("planner", "generator", "validator")
                    )

                conf = raw_data.get("relevance_confidence")
                reason = raw_data.get("relevance_reason")
                opp = raw_data.get("response_opportunity")
                current_topic = raw_data.get("current_topic")
                summary = raw_data.get("conversation_summary")
                intent = raw_data.get("detected_intent")
                suggestions = raw_data.get("suggestions", [])
                warnings = raw_data.get("validation_warnings", [])
                selected_model = raw_data.get("selected_model")

                understanding_result = {
                    "current_topic": current_topic,
                    "conversation_summary": summary,
                    "detected_intent": intent,
                }
                relevance_result = {
                    "should_suggest": actual_relevance,
                    "confidence": conf,
                    "reason": reason,
                    "response_opportunity": opp,
                }

                # Classification
                if scenario.expected_relevance:
                    classification = "TP" if actual_relevance else "FN"
                else:
                    classification = "FP" if actual_relevance else "TN"

                logger.info(
                    f"[{scenario.scenario_id}] Success ({latency_ms}ms, model={selected_model}) -> {classification} "
                    f"(status={status_str}, conf={conf}, suggestions={len(suggestions)})"
                )

                return ScenarioExecution(
                    scenario_id=scenario.scenario_id,
                    scenario_category=scenario.scenario_category,
                    scenario_type=scenario.scenario_type,
                    expected_relevance=scenario.expected_relevance,
                    execution_status="success",
                    http_status_code=200,
                    session_id=session_id,
                    actual_relevance=actual_relevance,
                    relevance_decision_evaluated=True,
                    relevance_confidence=conf,
                    relevance_reason=reason,
                    response_opportunity=opp,
                    detected_intent=intent,
                    current_topic=current_topic,
                    conversation_summary=summary,
                    understanding_result=understanding_result,
                    relevance_result=relevance_result,
                    suggestions=suggestions,
                    generated_suggestion_count=len(suggestions),
                    validation_warnings=warnings,
                    classification=classification,
                    failure_stage=None,
                    exception_category=None,
                    error_message=None,
                    error_detail=None,
                    selected_model=selected_model,
                    latency_ms=latency_ms,
                    raw_response=raw_data,
                )
            else:
                last_status_code = suggest_resp.status_code
                last_error_text = suggest_resp.text
                last_stage, last_category, _ = classify_error_details(
                    suggest_resp.status_code, suggest_resp.text, None
                )
                logger.warning(
                    f"[{scenario.scenario_id}] Attempt {attempt} failed ({last_stage}/{last_category}): {suggest_resp.status_code}"
                )

        except (httpx.TimeoutException, httpx.RequestError) as err:
            last_error = err
            last_stage, last_category, last_error_text = classify_error_details(
                None, None, err
            )
            logger.warning(
                f"[{scenario.scenario_id}] Attempt {attempt} network exception ({last_category}): {err}"
            )

        # Adaptive backoff (longer if rate limit)
        wait_time = (backoff_factor * 2.5 if last_category == "rate_limit" else backoff_factor) * attempt
        await asyncio.sleep(wait_time)

    # All retries exhausted -> Record failure separately
    latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
    logger.error(
        f"[{scenario.scenario_id}] FAILED after {retries} attempts [{last_stage}/{last_category}]: {last_error_text}"
    )

    return ScenarioExecution(
        scenario_id=scenario.scenario_id,
        scenario_category=scenario.scenario_category,
        scenario_type=scenario.scenario_type,
        expected_relevance=scenario.expected_relevance,
        execution_status="failed",
        http_status_code=last_status_code,
        session_id=None,
        actual_relevance=None,
        relevance_decision_evaluated=False,
        relevance_confidence=None,
        relevance_reason=None,
        response_opportunity=None,
        detected_intent=None,
        current_topic=None,
        conversation_summary=None,
        understanding_result=None,
        relevance_result=None,
        suggestions=[],
        generated_suggestion_count=0,
        validation_warnings=[],
        classification="ERROR",
        failure_stage=last_stage,
        exception_category=last_category,
        error_message=f"Request failed in stage '{last_stage}' with category '{last_category}'",
        error_detail=last_error_text or str(last_error),
        selected_model=None,
        latency_ms=latency_ms,
        raw_response=None,
    )


async def run_evaluation(
    base_url: str = DEFAULT_BASE_URL,
    dataset_path: Path | None = None,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    reports_dir: Path = DEFAULT_REPORTS_DIR,
    scenario_ids: list[str] | None = None,
    category: str | None = None,
    retries: int = 3,
    backoff_factor: float = 2.0,
    generate_report: bool = True,
) -> tuple[Path, EvaluationRun]:
    """Run full evaluation suite against the Conversa AI API."""
    run_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_id = f"eval_run_{run_timestamp}"

    results_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load scenarios
    scenarios = load_dataset(filepath=dataset_path, scenario_ids=scenario_ids, category=category)
    logger.info(f"Loaded {len(scenarios)} scenarios to evaluate.")

    # 2. Check health
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            health = await client.get(f"{base_url}/health/live")
            if health.status_code != 200:
                logger.warning(f"Health check warning: {health.status_code} - {health.text}")
        except Exception as e:
            logger.error(f"Cannot reach agent server at {base_url}: {e}")
            raise RuntimeError(f"Agent server not reachable at {base_url}: {e}") from e

        # 3. Execute scenarios sequentially to avoid rate-limiting the free router
        executions: list[ScenarioExecution] = []
        resolved_models: set[str] = set()

        for idx, scenario in enumerate(scenarios, start=1):
            logger.info(f"--- Running scenario {idx}/{len(scenarios)}: {scenario.scenario_id} ---")
            execution = await execute_scenario(
                client=client,
                base_url=base_url,
                scenario=scenario,
                retries=retries,
                backoff_factor=backoff_factor,
            )
            executions.append(execution)
            if execution.selected_model:
                resolved_models.add(execution.selected_model)

            # Pacing delay between scenarios
            await asyncio.sleep(1.5)

    # 4. Calculate separated automated metrics
    metrics = calculate_metrics(executions)

    model_resolved_str = (
        ", ".join(sorted(resolved_models)) if resolved_models else "openrouter/free (dynamic)"
    )

    # 5. Build EvaluationRun container
    evaluation_run = EvaluationRun(
        run_id=run_id,
        timestamp=datetime.utcnow().isoformat() + "Z",
        base_url=base_url,
        model_configured="openrouter/free",
        model_resolved=model_resolved_str,
        total_scenarios=len(executions),
        executions=executions,
        metrics=metrics,
    )

    # 6. Save Run Results JSON
    run_file = results_dir / f"{run_id}.json"
    with run_file.open("w", encoding="utf-8") as f:
        json.dump(evaluation_run.model_dump(), f, indent=2)
    logger.info(f"Run results saved to: {run_file}")

    # 7. Generate unreviewed human review templates
    usefulness_template = create_usefulness_review_template(executions)
    usefulness_path = results_dir / f"usefulness_{run_id}.json"
    save_usefulness_reviews(usefulness_template, usefulness_path)
    logger.info(f"Usefulness review template saved ({len(usefulness_template)} items): {usefulness_path}")

    scenarios_by_id = {s.scenario_id: s for s in scenarios}
    factuality_template = create_factuality_review_template(executions, scenarios_by_id)
    factuality_path = results_dir / f"factuality_{run_id}.json"
    save_factuality_reviews(factuality_template, factuality_path)
    logger.info(f"Factuality review template saved ({len(factuality_template)} items): {factuality_path}")

    # 8. Generate Final Evaluation Reports
    if generate_report:
        standard_report_path = reports_dir / "evaluation_report.md"
        generate_report_file(
            run_path=run_file,
            report_output_path=standard_report_path,
            usefulness_path=usefulness_path,
            factuality_path=factuality_path,
        )

        archive_report_path = reports_dir / f"evaluation_report_{run_timestamp}.md"
        generate_report_file(
            run_path=run_file,
            report_output_path=archive_report_path,
            usefulness_path=usefulness_path,
            factuality_path=factuality_path,
        )
        logger.info(f"Markdown report generated at: {standard_report_path}")

    # 9. Print Terminal Summary
    rel = metrics.relevance
    gen = metrics.generation
    infra = metrics.infrastructure

    print("\n" + "=" * 65)
    print("CONVERSA AI - EVALUATION BENCHMARK SUMMARY")
    print("=" * 65)
    print(f"Run ID:                    {evaluation_run.run_id}")
    print(f"Resolved Models:           {evaluation_run.model_resolved}")
    print("-" * 65)
    print("[1. Infrastructure Reliability]")
    print(f"  Total Scenarios:         {infra.total_scenarios}")
    print(f"  Successful Executions:   {infra.successful_executions} ({infra.successful_execution_rate * 100:.1f}%)")
    print(f"  Failed Executions:       {infra.failed_executions} ({infra.failed_execution_rate * 100:.1f}%)")
    if infra.failure_rate_by_stage:
        print(f"  Failures by Stage:       {dict(infra.failure_rate_by_stage)}")
    if infra.failure_categories:
        print(f"  Failure Categories:      {dict(infra.failure_categories)}")
    print("-" * 65)
    print("[2. Relevance Classification (Evaluated from completed decisions)]")
    print(f"  Relevance Evaluations:   {rel.total_relevance_evaluations}")
    print(f"  True Positives (TP):     {rel.tp}")
    print(f"  False Positives (FP):    {rel.fp}")
    print(f"  True Negatives (TN):     {rel.tn}")
    print(f"  False Negatives (FN):    {rel.fn}")
    print(f"  Relevance Precision:     {rel.precision * 100 if rel.precision is not None else 0.0:.1f}%")
    print(f"  Relevance Recall:        {rel.recall * 100 if rel.recall is not None else 0.0:.1f}%")
    print(f"  False Positive Rate:     {rel.false_positive_rate * 100 if rel.false_positive_rate is not None else 0.0:.1f}%")
    print(f"  Missed Opportunity Rate: {rel.missed_opportunity_rate * 100 if rel.missed_opportunity_rate is not None else 0.0:.1f}%")
    print(f"  F1 Score:                {rel.f1_score:.4f}" if rel.f1_score is not None else "0.0000")
    print("-" * 65)
    print("[3. End-to-End Suggestion Generation]")
    print(f"  Positive Scenarios Eval: {gen.positive_scenarios_evaluated}")
    print(f"  With Valid Suggestions:  {gen.positive_scenarios_with_suggestions}")
    print(f"  Relevant But No Sug:     {gen.positive_scenarios_relevant_but_no_suggestion}")
    print(f"  Generation Failure Rate: {gen.generation_failure_rate * 100 if gen.generation_failure_rate is not None else 0.0:.1f}%")
    print(f"  Avg Sug per Pos Scenario:{gen.avg_suggestions_per_successful_positive:.2f}" if gen.avg_suggestions_per_successful_positive is not None else "N/A")
    print("=" * 65 + "\n")

    return run_file, evaluation_run


def main():
    parser = argparse.ArgumentParser(description="Conversa AI Evaluation Suite Runner")
    parser.add_argument("--base-url", type=str, default=DEFAULT_BASE_URL, help=f"Agent server URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument("--dataset", type=Path, default=None, help="Path to dataset.jsonl")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR, help="Directory to save JSON results")
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR, help="Directory to save Markdown reports")
    parser.add_argument("--scenario-id", action="append", help="Specific scenario ID(s) to run (can be repeated)")
    parser.add_argument("--category", choices=["positive", "negative"], help="Filter scenarios by category")
    parser.add_argument("--retries", type=int, default=3, help="Number of retry attempts per request")
    parser.add_argument("--backoff", type=float, default=2.0, help="Exponential backoff factor")
    parser.add_argument("--no-report", action="store_true", help="Skip generating markdown report")

    args = parser.parse_args()

    asyncio.run(
        run_evaluation(
            base_url=args.base_url,
            dataset_path=args.dataset,
            results_dir=args.results_dir,
            reports_dir=args.reports_dir,
            scenario_ids=args.scenario_id,
            category=args.category,
            retries=args.retries,
            backoff_factor=args.backoff,
            generate_report=not args.no_report,
        )
    )


if __name__ == "__main__":
    main()
