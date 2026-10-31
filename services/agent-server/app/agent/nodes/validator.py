from app.agent.state import AgentState


async def validator_node(state: AgentState):

    suggestions = state.get(
        "suggestions", []
    )

    validated = []
    warnings = []

    seen = set()

    for suggestion in suggestions:

        text = suggestion.get(
            "text", ""
        ).strip()

        rationale = suggestion.get(
            "rationale", ""
        ).strip()

        if len(text) < 10:
            warnings.append(
                "Suggestion was too short."
            )
            continue

        if len(text) > 1000:
            warnings.append(
                "Suggestion exceeded the length limit."
            )
            continue

        if not rationale:
            warnings.append(
                "Suggestion had no relevance explanation."
            )
            continue

        normalized = " ".join(
            text.lower().split()
        )

        if normalized in seen:
            warnings.append(
                "Duplicate suggestion removed."
            )
            continue

        seen.add(normalized)

        validated.append({
            **suggestion,
            "text": text,
            "rationale": rationale
        })

    status = (
        "suggestions_ready"
        if validated
        else "no_suggestion"
    )

    return {
        "suggestions": validated,
        "validation_warnings": warnings,
        "status": status,
        "stage_reached": "validator",
    }