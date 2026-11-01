"""Usefulness review module for Conversa AI evaluation framework.

Manages human usefulness evaluations on a 1-5 scale across 7 qualitative dimensions:
1. Relevance to ongoing conversation
2. Alignment with user objective
3. Alignment with communication goals
4. Practical value
5. Natural conversational language
6. Conciseness
7. Non-repetition

Also supports an optional secondary LLM evaluation (strictly labeled as secondary).
"""

import argparse
from datetime import datetime
import json
from pathlib import Path
from typing import Sequence

from tests.evaluation.metrics import compute_usefulness_summary
from tests.evaluation.schemas import (
    EvaluationRun,
    ScenarioExecution,
    UsefulnessReviewItem,
    UsefulnessSummary,
)

SCORE_DESCRIPTIONS = {
    1: "Completely irrelevant or unhelpful",
    2: "Weak contribution with minor value or awkward phrasing",
    3: "Partially useful; addresses the topic but has notable omissions or repetition",
    4: "Useful and relevant; good contribution aligned with user goals",
    5: "Highly useful, immediate-use contribution with excellent nuance and timing",
}


def create_usefulness_review_template(
    executions: Sequence[ScenarioExecution],
) -> list[UsefulnessReviewItem]:
    """Generate empty human review records for all suggestions generated in a run."""
    items: list[UsefulnessReviewItem] = []
    for exec_item in executions:
        for idx, suggestion in enumerate(exec_item.suggestions):
            item = UsefulnessReviewItem(
                scenario_id=exec_item.scenario_id,
                suggestion_index=idx,
                suggestion_text=suggestion.get("text", ""),
                response_type=suggestion.get("response_type", "direct_answer"),
                rationale=suggestion.get("rationale", ""),
                score=None,
                relevance_to_conversation=None,
                alignment_with_objective=None,
                alignment_with_communication_goals=None,
                practical_value=None,
                natural_conversational_language=None,
                conciseness=None,
                non_repetition=None,
                notes=None,
                reviewer=None,
                reviewed_at=None,
                is_secondary_llm=False,
            )
            items.append(item)
    return items


def save_usefulness_reviews(items: list[UsefulnessReviewItem], target_path: Path) -> None:
    """Save usefulness review items to a formatted JSON file."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as f:
        json.dump([item.model_dump() for item in items], f, indent=2)


def load_usefulness_reviews(filepath: Path) -> list[UsefulnessReviewItem]:
    """Load usefulness review items from a JSON file."""
    if not filepath.is_file():
        return []
    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return [UsefulnessReviewItem.model_validate(raw) for raw in data]


def run_interactive_usefulness_review(
    items: list[UsefulnessReviewItem],
    reviewer_name: str,
    output_path: Path,
) -> list[UsefulnessReviewItem]:
    """Interactive CLI review prompt for human evaluators."""
    print("\n" + "=" * 70)
    print("CONVERSA AI - HUMAN USEFULNESS REVIEW TOOL")
    print("=" * 70)
    print(f"Reviewer: {reviewer_name}")
    print("For each suggestion, evaluate usefulness on a 1-5 scale:")
    for score, desc in SCORE_DESCRIPTIONS.items():
        print(f"  {score}: {desc}")
    print("Type 'skip' to skip an item, or 'quit' to save and exit.\n")

    completed = 0
    for idx, item in enumerate(items, start=1):
        if item.score is not None:
            continue  # Already reviewed

        print("-" * 70)
        print(f"[{idx}/{len(items)}] Scenario ID: {item.scenario_id} (Suggestion #{item.suggestion_index + 1})")
        print(f"Response Type: {item.response_type}")
        print(f"Rationale:     {item.rationale}")
        print(f"Suggestion:    \"{item.suggestion_text}\"")
        print("-" * 70)

        # Prompt for score
        while True:
            val = input("Usefulness score (1-5, 'skip', 'quit'): ").strip().lower()
            if val in ("quit", "q"):
                save_usefulness_reviews(items, output_path)
                print(f"\nSaved progress to {output_path}")
                return items
            if val in ("skip", "s"):
                break
            if val in ("1", "2", "3", "4", "5"):
                item.score = int(val)
                item.reviewer = reviewer_name
                item.reviewed_at = datetime.utcnow().isoformat() + "Z"

                # Quick criteria queries (y/n)
                rel = input("  Relevance to ongoing conversation? (y/n): ").strip().lower() == "y"
                obj = input("  Aligned with user objective? (y/n): ").strip().lower() == "y"
                goal = input("  Aligned with communication goals? (y/n): ").strip().lower() == "y"
                prac = input("  Practical value? (y/n): ").strip().lower() == "y"
                nat = input("  Natural conversational language? (y/n): ").strip().lower() == "y"
                conc = input("  Concise? (y/n): ").strip().lower() == "y"
                rep = input("  Non-repetitive? (y/n): ").strip().lower() == "y"
                notes = input("  Review notes (optional): ").strip()

                item.relevance_to_conversation = rel
                item.alignment_with_objective = obj
                item.alignment_with_communication_goals = goal
                item.practical_value = prac
                item.natural_conversational_language = nat
                item.conciseness = conc
                item.non_repetition = rep
                if notes:
                    item.notes = notes

                completed += 1
                save_usefulness_reviews(items, output_path)
                break
            print("Invalid input. Enter a number between 1 and 5, 'skip', or 'quit'.")

    print(f"\nReview session complete. {completed} items reviewed and saved to {output_path}.")
    return items


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conversa AI Suggestion Usefulness Review Tool")
    parser.add_argument("--run-file", type=Path, help="Path to evaluation run JSON file")
    parser.add_argument("--review-file", type=Path, help="Path to usefulness review JSON file")
    parser.add_argument("--reviewer", type=str, default="Human Reviewer", help="Name of human reviewer")
    parser.add_argument("--interactive", action="store_true", help="Launch interactive CLI review")
    parser.add_argument("--summary", action="store_true", help="Print summary of review file")

    args = parser.parse_args()

    if args.summary and args.review_file:
        items = load_usefulness_reviews(args.review_file)
        summary = compute_usefulness_summary(items)
        print(json.dumps(summary.model_dump(), indent=2))
    elif args.interactive and args.run_file:
        with args.run_file.open("r", encoding="utf-8") as f:
            run_data = EvaluationRun.model_validate(json.load(f))
        out_path = args.review_file or (args.run_file.parent / f"usefulness_{args.run_file.stem}.json")
        items = load_usefulness_reviews(out_path) if out_path.is_file() else create_usefulness_review_template(run_data.executions)
        run_interactive_usefulness_review(items, args.reviewer, out_path)
    else:
        parser.print_help()
