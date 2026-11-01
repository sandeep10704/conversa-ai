import re
from typing import Any

from app.agent.state import AgentState

VALID_RESPONSE_TYPES = {
    "direct_answer",
    "technical_explanation",
    "supporting_point",
    "polite_disagreement",
    "example",
    "follow_up_question",
    "perspective",
}

STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are",
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
    "but", "by", "can", "could", "did", "do", "does", "doing", "down", "during", "each",
    "few", "for", "from", "further", "had", "has", "have", "having", "he", "her", "here",
    "him", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just",
    "me", "more", "most", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
    "only", "or", "other", "our", "ours", "out", "over", "own", "same", "she", "should",
    "so", "some", "such", "than", "that", "the", "their", "theirs", "them", "then",
    "there", "these", "they", "this", "those", "through", "to", "too", "under", "until",
    "up", "very", "was", "we", "were", "what", "when", "where", "which", "while", "who",
    "whom", "why", "with", "would", "you", "your", "yours"
}


def _extract_numeric_tokens(text: str) -> set[str]:
    """Extract numeric values (integers or decimals) from text."""
    return set(re.findall(r"\b\d+(?:\.\d+)?\b", text))


def _extract_metric_quantities(text: str) -> list[tuple[str, str]]:
    """Extract (number, unit) pairs from text."""
    pattern = r"\b(\d+(?:\.\d+)?)\s*(ms|milliseconds|seconds|s|%|min|minutes|hz|khz|kbps|mb|gb)\b"
    return re.findall(pattern, text, re.IGNORECASE)


def _tokenize_content(text: str) -> set[str]:
    """Extract meaningful lowercase alphanumeric tokens of at least 2 characters, excluding stop words."""
    words = re.findall(r"\b[a-zA-Z0-9]{2,}\b", text.lower())
    return {w for w in words if w not in STOP_WORDS}


def _jaccard_similarity(tokens1: set[str], tokens2: set[str]) -> float:
    """Compute Jaccard similarity between two token sets."""
    if not tokens1 or not tokens2:
        return 0.0
    return len(tokens1 & tokens2) / len(tokens1 | tokens2)


def _containment_similarity(tokens1: set[str], tokens2: set[str]) -> float:
    """Compute overlap / containment coefficient between two token sets."""
    if not tokens1 or not tokens2:
        return 0.0
    return len(tokens1 & tokens2) / min(len(tokens1), len(tokens2))


async def validator_node(state: AgentState):
    """Deterministic validation of agent suggestions.

    Enforces:
    - Non-empty text and rationale
    - Length limits (10 to 1000 characters)
    - Valid response types
    - Absence of generic platitudes
    - Factual grounding: no ungrounded metrics/thresholds or invented timing margins
    - Conceptual consistency: no conflation of VAD frame latency with silence duration
    - Repetition prevention against previous suggestions (exact match and idea overlap)
    """
    suggestions = state.get("suggestions", [])
    previous_suggestions = state.get("previous_suggestions", [])

    # Compile context text to find all legitimately mentioned numbers
    messages = state.get("messages", [])
    conv_text = " ".join([str(m.get("text", "")) for m in messages])
    prev_texts = [str(s.get("suggestion_text") or s.get("text", "")) for s in previous_suggestions]
    context_text = " ".join([
        conv_text,
        " ".join(prev_texts),
        str(state.get("situation", "")),
        str(state.get("objective", "")),
        " ".join(state.get("communication_goals", []) or []),
        " ".join(state.get("constraints", []) or []),
    ])
    allowed_numbers = _extract_numeric_tokens(context_text)

    # Prepare previous suggestions token sets for repetition checks
    prev_sugg_data = []
    for prev in previous_suggestions:
        p_text = (prev.get("suggestion_text") or prev.get("text") or "").strip()
        if p_text:
            prev_sugg_data.append({
                "normalized": " ".join(p_text.lower().split()),
                "tokens": _tokenize_content(p_text),
                "text": p_text,
            })

    validated = []
    warnings = []
    seen_in_batch = set()

    for suggestion in suggestions:
        if not isinstance(suggestion, dict):
            warnings.append("Suggestion is not a dictionary.")
            continue

        text = (suggestion.get("text") or suggestion.get("suggestion_text") or "").strip()
        rationale = (suggestion.get("rationale") or "").strip()
        response_type = suggestion.get("response_type", "supporting_point")

        # 1. Non-empty & length bounds
        if not text or len(text) < 10:
            warnings.append("Suggestion was too short or empty.")
            continue

        if len(text) > 1000:
            warnings.append("Suggestion exceeded the length limit.")
            continue

        if not rationale:
            warnings.append("Suggestion had no relevance explanation.")
            continue

        # 2. Response type validity
        if response_type not in VALID_RESPONSE_TYPES:
            warnings.append(f"Invalid response_type '{response_type}'.")
            continue

        normalized = " ".join(text.lower().split())

        # 3. In-batch duplicate check
        if normalized in seen_in_batch:
            warnings.append("Duplicate suggestion removed from current response.")
            continue

        # 4. Generic platitude check
        # Catches empty boilerplate like "Consider reviewing the asynchronous event pipeline latency metrics."
        if re.search(r"^(?:consider\s+(?:reviewing|monitoring|checking)|we\s+should\s+(?:review|monitor|check))\s+(?:the\s+)?(?:asynchronous\s+)?(?:event\s+pipeline\s+)?latency\s+metrics\.?$", text, re.IGNORECASE):
            warnings.append("Generic platitude rejected: suggestion lacks substantive technical contribution.")
            continue

        # 5. Unsupported numeric recommendations / ungrounded thresholds / margins
        has_grounding_issue = False
        metric_quantities = _extract_metric_quantities(text)
        for num_str, unit in metric_quantities:
            if num_str not in allowed_numbers:
                warnings.append(
                    f"Unsupported numeric recommendation: ungrounded metric/threshold '{num_str} {unit}' "
                    "not present in conversation context."
                )
                has_grounding_issue = True
                break

        # Check for invented safety margins or unmeasured buffers
        if re.search(r"\b(safety\s+margin|timing\s+buffer|margin\s+of\s+~?\d+)\b", text, re.IGNORECASE):
            if not any(k in context_text.lower() for k in ["safety margin", "margin", "buffer"]):
                warnings.append("Unsupported recommendation: proposes ungrounded safety margin without empirical calibration.")
                has_grounding_issue = True

        # 6. Conceptual / timing conflation check
        # Conflating VAD frame processing latency with speech-end silence detection duration
        if re.search(r"\b(vad|silero)\s+(?:silence|speech[- ]end)\s+(?:detection|duration|threshold)\b", text, re.IGNORECASE) or \
           re.search(r"\b50\s*(?:ms|milliseconds)\s+(?:silence|threshold|speech[- ]end)\b", text, re.IGNORECASE):
            warnings.append("Conceptual conflation: confuses VAD frame latency with speech-end silence detection duration.")
            has_grounding_issue = True

        if has_grounding_issue:
            continue

        # 7. Repetition prevention against session history
        candidate_tokens = _tokenize_content(text)
        is_repetition = False

        for prev in prev_sugg_data:
            # Exact normalized match
            if normalized == prev["normalized"]:
                warnings.append("Duplicate suggestion: exact match with previous suggestion.")
                is_repetition = True
                break

            # Underlying idea / token overlap comparison
            inter = candidate_tokens & prev["tokens"]
            jaccard = _jaccard_similarity(candidate_tokens, prev["tokens"])
            containment = _containment_similarity(candidate_tokens, prev["tokens"])

            if jaccard >= 0.40 or (len(inter) >= 3 and containment >= 0.50):
                warnings.append(
                    f"Repeated suggestion: underlying idea already addressed in previous suggestion (similarity: {jaccard:.2f}, overlap: {containment:.2f})."
                )
                is_repetition = True
                break

        if is_repetition:
            continue

        # Validated suggestion
        seen_in_batch.add(normalized)
        validated.append({
            **suggestion,
            "text": text,
            "suggestion_text": text,
            "rationale": rationale,
            "response_type": response_type,
        })

    status = "suggestions_ready" if validated else "no_suggestion"

    return {
        "suggestions": validated,
        "validation_warnings": warnings,
        "status": status,
        "stage_reached": "validator",
    }
