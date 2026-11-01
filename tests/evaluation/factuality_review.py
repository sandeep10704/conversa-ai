"""Factual accuracy review module for Conversa AI evaluation framework.

Extracts, tracks, and evaluates factual claims in generated suggestions:
- Supported: Corroborated by conversation context or scenario reference facts.
- Unsupported: Plausible claim lacking backing in facts or context.
- Incorrect: Demonstrably false statement contradicting reference facts or reality.
- Not applicable: Conversational filler, polite greetings, or questions without factual assertions.

Factual Accuracy = Supported Claims / Total Verifiable Claims
where Total Verifiable Claims = Supported + Unsupported + Incorrect.
"""

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Sequence

from tests.evaluation.metrics import compute_factuality_summary
from tests.evaluation.schemas import (
    EvaluationRun,
    FactualityReviewItem,
    FactualitySummary,
    Scenario,
    ScenarioExecution,
)


def extract_candidate_claims(suggestion_text: str) -> list[str]:
    """Split suggestion text into sentence-level candidate factual claims."""
    # Split on sentence boundaries (. ! ?) while handling abbreviations
    sentences = re.split(r"(?<=[.!?])\s+", suggestion_text.strip())
    claims: list[str] = []

    filler_prefixes = [
        "hello", "hi ", "hey ", "thanks", "i agree", "sounds good",
        "i think", "in my opinion", "let me suggest", "how about", "what if"
    ]

    for s in sentences:
        s_clean = s.strip()
        if not s_clean or len(s_clean) < 8:
            continue
        # Retain candidate sentences
        claims.append(s_clean)

    return claims if claims else [suggestion_text.strip()]


def create_factuality_review_template(
    executions: Sequence[ScenarioExecution],
    scenarios_by_id: dict[str, Scenario] | None = None,
) -> list[FactualityReviewItem]:
    """Generate unreviewed claim templates for suggestions generated in a run."""
    items: list[FactualityReviewItem] = []

    for exec_item in executions:
        scenario = scenarios_by_id.get(exec_item.scenario_id) if scenarios_by_id else None
        ref_facts = scenario.reference_facts if scenario else []

        for sug_idx, suggestion in enumerate(exec_item.suggestions):
            text = suggestion.get("text", "")
            claims = extract_candidate_claims(text)

            for claim_idx, claim in enumerate(claims):
                # Pre-match reference fact if an obvious keyword matches
                matched_fact = None
                for rf in ref_facts:
                    # Quick heuristic match for display
                    shared_words = set(re.findall(r"\w+", claim.lower())) & set(re.findall(r"\w+", rf.lower()))
                    if len(shared_words) >= 3:
                        matched_fact = rf
                        break

                item = FactualityReviewItem(
                    scenario_id=exec_item.scenario_id,
                    suggestion_index=sug_idx,
                    claim_index=claim_idx,
                    claim_text=claim,
                    classification=None,
                    explanation=None,
                    reference_fact_matched=matched_fact,
                    reviewer=None,
                    reviewed_at=None,
                )
                items.append(item)

    return items


def save_factuality_reviews(items: list[FactualityReviewItem], target_path: Path) -> None:
    """Save factual accuracy review items to JSON."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as f:
        json.dump([item.model_dump() for item in items], f, indent=2)


def load_factuality_reviews(filepath: Path) -> list[FactualityReviewItem]:
    """Load factual accuracy review items from JSON."""
    if not filepath.is_file():
        return []
    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return [FactualityReviewItem.model_validate(raw) for raw in data]


def run_interactive_factuality_review(
    items: list[FactualityReviewItem],
    reviewer_name: str,
    output_path: Path,
) -> list[FactualityReviewItem]:
    """Interactive CLI review prompt for claim-level factual accuracy."""
    print("\n" + "=" * 70)
    print("CONVERSA AI - FACTUAL ACCURACY REVIEW TOOL")
    print("=" * 70)
    print(f"Reviewer: {reviewer_name}")
    print("Classify each claim into one of four categories:")
    print("  [s] Supported: Directly backed by context or reference facts.")
    print("  [u] Unsupported: Plausible claim lacking explicit verification.")
    print("  [i] Incorrect: Factually wrong or contradicted by facts.")
    print("  [n] Not applicable: Conversational filler, greeting, or pure question.")
    print("Type 'skip' to skip, or 'quit' to save and exit.\n")

    completed = 0
    for idx, item in enumerate(items, start=1):
        if item.classification is not None:
            continue

        print("-" * 70)
        print(f"[{idx}/{len(items)}] Scenario ID: {item.scenario_id} | Sug #{item.suggestion_index + 1} | Claim #{item.claim_index + 1}")
        print(f"Claim:          \"{item.claim_text}\"")
        if item.reference_fact_matched:
            print(f"Reference Fact:  {item.reference_fact_matched}")
        print("-" * 70)

        while True:
            val = input("Classification [s=Supported / u=Unsupported / i=Incorrect / n=N/A / skip / quit]: ").strip().lower()
            if val in ("quit", "q"):
                save_factuality_reviews(items, output_path)
                print(f"\nSaved progress to {output_path}")
                return items
            if val in ("skip", "skip"):
                break

            mapping = {
                "s": "supported",
                "u": "unsupported",
                "i": "incorrect",
                "n": "not_applicable",
            }
            if val in mapping:
                classification = mapping[val]
                explanation = input("  Brief explanation: ").strip()

                item.classification = classification
                item.explanation = explanation if explanation else None
                item.reviewer = reviewer_name
                item.reviewed_at = datetime.utcnow().isoformat() + "Z"

                completed += 1
                save_factuality_reviews(items, output_path)
                break

            print("Invalid input. Enter s, u, i, n, skip, or quit.")

    print(f"\nFactuality review session complete. {completed} items reviewed and saved to {output_path}.")
    return items


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conversa AI Factual Accuracy Review Tool")
    parser.add_argument("--run-file", type=Path, help="Path to evaluation run JSON file")
    parser.add_argument("--review-file", type=Path, help="Path to factuality review JSON file")
    parser.add_argument("--reviewer", type=str, default="Human Evaluator", help="Name of evaluator")
    parser.add_argument("--interactive", action="store_true", help="Launch interactive CLI review")
    parser.add_argument("--summary", action="store_true", help="Print summary of review file")

    args = parser.parse_args()

    if args.summary and args.review_file:
        items = load_factuality_reviews(args.review_file)
        summary = compute_factuality_summary(items)
        print(json.dumps(summary.model_dump(), indent=2))
    elif args.interactive and args.run_file:
        with args.run_file.open("r", encoding="utf-8") as f:
            run_data = EvaluationRun.model_validate(json.load(f))
        out_path = args.review_file or (args.run_file.parent / f"factuality_{args.run_file.stem}.json")
        items = load_factuality_reviews(out_path) if out_path.is_file() else create_factuality_review_template(run_data.executions)
        run_interactive_factuality_review(items, args.reviewer, out_path)
    else:
        parser.print_help()
