"""Compact context selector for personalized suggestion generation.

Selects only the discussion-relevant fields from participant_context based on the
current discussion topic and conversation context, filtering out irrelevant keywords
and minimizing prompt token overhead.
"""

import re
from typing import Any, List, Optional, Set

STOPWORDS: Set[str] = {
    "a", "an", "the", "and", "or", "in", "on", "at", "to", "for", "of", "with",
    "by", "from", "up", "about", "into", "over", "after", "is", "are", "was",
    "were", "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "our", "my", "your", "their", "we", "i", "you", "they", "it", "this", "that",
    "as", "so", "if", "not", "no", "yes", "but", "what", "which", "who", "when"
}


def _tokenize(text: str) -> Set[str]:
    """Extract lowercased alphanumeric tokens excluding common stopwords."""
    words = re.findall(r"\b[a-zA-Z0-9_\-+#.]+\b", text.lower())
    return {w for w in words if w not in STOPWORDS and len(w) > 1}


def select_compact_participant_context(
    participant_context: Optional[dict[str, Any]],
    current_topic: Optional[str] = None,
    conversation_summary: Optional[str] = None,
    conversation_text: Optional[str] = None,
    response_opportunity: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Extract a concise, discussion-relevant slice of participant context.

    Rules:
    - Never include preferred_name (spoken suggestions must not address oneself).
    - Always include constraints if present (critical safety guardrails).
    - Include role if present (provides professional grounding and perspective).
    - Include session_goals if present (scoped to this specific session).
    - Filter expertise_keywords to only those with semantic relevance to the
      current topic, discussion text, summary, or response opportunity.
    - Include responsibilities only if topically relevant to the discussion.
    - Return None if participant_context is missing or no fields remain.
    """
    if not participant_context or not isinstance(participant_context, dict):
        return None

    # Build search corpus for relevance matching
    corpus_parts = [
        current_topic or "",
        conversation_summary or "",
        conversation_text or "",
        response_opportunity or "",
    ]
    raw_corpus = " ".join(corpus_parts).strip()
    corpus_tokens = _tokenize(raw_corpus)
    raw_corpus_lower = raw_corpus.lower()

    compact: dict[str, Any] = {}

    # 1. Role: Include if present
    role = participant_context.get("role")
    if role and isinstance(role, str) and role.strip():
        compact["role"] = role.strip()

    # 2. Responsibilities: Include if topically relevant or if discussion corpus is empty
    resp = participant_context.get("responsibilities")
    if resp and isinstance(resp, str) and resp.strip():
        resp_clean = resp.strip()
        if not corpus_tokens:
            compact["responsibilities"] = resp_clean
        else:
            resp_tokens = _tokenize(resp_clean)
            if (resp_tokens & corpus_tokens) or any(t in raw_corpus_lower for t in resp_tokens):
                compact["responsibilities"] = resp_clean

    # 3. Expertise Keywords: Filter to only discussion-relevant keywords
    keywords = participant_context.get("expertise_keywords", [])
    if isinstance(keywords, list) and keywords:
        selected_keywords: List[str] = []
        for kw in keywords:
            if not isinstance(kw, str) or not kw.strip():
                continue
            kw_clean = kw.strip()
            kw_lower = kw_clean.lower()
            if not corpus_tokens:
                selected_keywords.append(kw_clean)
            else:
                kw_tokens = _tokenize(kw_clean)
                if kw_lower in raw_corpus_lower or (kw_tokens & corpus_tokens):
                    selected_keywords.append(kw_clean)

        if selected_keywords:
            compact["expertise_keywords"] = selected_keywords

    # 4. Session Goals: Include if present
    goals = participant_context.get("session_goals")
    if goals and isinstance(goals, str) and goals.strip():
        compact["session_goals"] = goals.strip()

    # 5. Constraints: ALWAYS preserve constraints as safety boundaries
    constraints = participant_context.get("constraints")
    if constraints and isinstance(constraints, str) and constraints.strip():
        compact["constraints"] = constraints.strip()

    if not compact:
        return None

    return compact
