"""Dataset loader and validator for Conversa AI evaluation framework.

Loads and validates evaluation scenarios from dataset.jsonl according to
the defined Pydantic schema.
"""

import json
from pathlib import Path
from typing import Any, Literal

from tests.evaluation.schemas import Scenario

DEFAULT_DATASET_PATH = (
    Path(__file__).resolve().parent / "dataset.jsonl"
)


class DatasetLoadError(Exception):
    """Raised when evaluation dataset cannot be loaded or is invalid."""


def load_dataset(
    filepath: str | Path | None = None,
    scenario_ids: list[str] | None = None,
    category: Literal["positive", "negative"] | str | None = None,
) -> list[Scenario]:
    """Load and validate scenarios from the JSONL dataset file.

    Args:
        filepath: Path to dataset.jsonl. Defaults to tests/evaluation/dataset.jsonl.
        scenario_ids: Optional list of specific scenario IDs to filter by.
        category: Optional category filter ('positive' or 'negative').

    Returns:
        List of validated Scenario objects.

    Raises:
        DatasetLoadError: If file not found, unparseable, invalid schema, or duplicate IDs.
    """
    path = Path(filepath) if filepath else DEFAULT_DATASET_PATH

    if not path.is_file():
        raise DatasetLoadError(f"Evaluation dataset file not found at: {path}")

    scenarios: list[Scenario] = []
    seen_ids: set[str] = set()

    scenario_ids_set = set(scenario_ids) if scenario_ids else None

    with path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line_str = line.strip()
            if not line_str:
                continue

            try:
                data = json.loads(line_str)
            except json.JSONDecodeError as exc:
                raise DatasetLoadError(
                    f"Line {line_num} in {path} contains invalid JSON: {exc}"
                ) from exc

            try:
                scenario = Scenario.model_validate(data)
            except Exception as exc:
                raise DatasetLoadError(
                    f"Line {line_num} in {path} failed schema validation: {exc}"
                ) from exc

            if scenario.scenario_id in seen_ids:
                raise DatasetLoadError(
                    f"Duplicate scenario_id detected at line {line_num}: {scenario.scenario_id}"
                )
            seen_ids.add(scenario.scenario_id)

            # Filtering
            if scenario_ids_set and scenario.scenario_id not in scenario_ids_set:
                continue

            if category and scenario.scenario_category != category:
                continue

            scenarios.append(scenario)

    return scenarios


def get_scenario(
    scenario_id: str,
    filepath: str | Path | None = None
) -> Scenario | None:
    """Retrieve a single scenario by its ID."""
    scenarios = load_dataset(filepath=filepath, scenario_ids=[scenario_id])
    return scenarios[0] if scenarios else None


def validate_dataset_integrity(scenarios: list[Scenario]) -> dict[str, Any]:
    """Check dataset balance and integrity constraints.

    Returns:
        Dictionary with count statistics and validation status.
    """
    total = len(scenarios)
    positive = sum(1 for s in scenarios if s.scenario_category == "positive")
    negative = sum(1 for s in scenarios if s.scenario_category == "negative")

    # Check for speaker name leaks (e.g., 'Alice:', 'Speaker 1:', 'Bob:')
    speaker_leak_warnings: list[str] = []
    for s in scenarios:
        for idx, msg in enumerate(s.messages):
            first_word = msg.split(":")[0] if ":" in msg else ""
            if first_word and len(first_word) <= 15 and not first_word.startswith("http"):
                # Potential speaker label if format is Name: ...
                if any(word in first_word.lower() for word in ["speaker", "person", "user", "agent"]):
                    speaker_leak_warnings.append(
                        f"Scenario {s.scenario_id} msg {idx} has possible speaker prefix: '{first_word}:'"
                    )

    return {
        "total_scenarios": total,
        "positive_scenarios": positive,
        "negative_scenarios": negative,
        "is_balanced": (positive == 12 and negative == 12),
        "speaker_leak_warnings": speaker_leak_warnings,
    }
