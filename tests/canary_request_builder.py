"""Conversa AI — Nemotron Inference Reliability: Canary Request Builder.

Provides offline, validated request construction and parsing for controlled canary runs:
- Exact model ID enforcement (nvidia/nemotron-3.5-lightning:free).
- Model-metadata-driven reasoning configuration (safe, unblinded parameter handling).
- Concise, grounding-preserving system prompt (~50% shorter than canary baseline).
- Zero automatic retries (max_retries=0).
- Zero parser repair calls (allow_repair=False).
- Strict offline parse validation without outbound network requests.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

logger = logging.getLogger("canary_request_builder")

# Exact model identifier required for canary validation
EXACT_CANARY_MODEL_ID = "nvidia/nemotron-3.5-lightning:free"

# Default completion budget evaluated for short structured suggestion
DEFAULT_CANARY_MAX_TOKENS = 1000

# Refined concise system prompt (~160 words vs 336 words in previous canary)
# Preserves all 6 mandatory grounding, distinction, and repetition rules.
CONCISE_SYSTEM_PROMPT = """You are the response generator for Conversa AI.
Help the user contribute naturally to the live group discussion.

Generate ONE concise spoken suggestion.

Rules:
1. Spoken Style: Natural, direct, and concise for speaking aloud in a meeting.
2. Factual Grounding: Use only facts stated in the conversation. Treat participant-stated figures (e.g., ~50ms Silero VAD, ~180ms Whisper Tiny) as conversational observations, not verified system benchmarks.
3. Latency & Silence Distinction: Do not confuse VAD chunk processing latency with speech silence thresholds for turn-taking.
4. No Invented Numbers or Architecture: Do not invent unmentioned timing thresholds, safety margins, or components. If an exact threshold is unmeasured, recommend measuring, profiling, or calibrating instead of asserting an invented figure.
5. Anti-Repetition: Check previous_suggestions; do not repeat ideas already suggested.
6. Format: Output only a valid JSON object matching:
{"suggestions": [{"response_type": "direct_answer", "text": "...", "rationale": "...", "follow_up_question": null}]}

Do not include hidden reasoning or conversational commentary. Output only the JSON object."""


def determine_reasoning_config(
    model_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Determine safe reasoning configuration based on confirmed model metadata.

    Rules:
    - If model_metadata is missing or None -> returns None (cannot confirm support).
    - If reasoning is mandatory (reasoning.mandatory == True) -> returns None (effort: 'none' is invalid).
    - If supported_efforts is explicitly present and contains 'none' -> returns {'effort': 'none'}.
    - If supported_efforts is omitted/null or does NOT contain 'none' -> returns None (ambiguous/unconfirmed).

    Do NOT blindly pass reasoning={'effort': 'none'} or reasoning={'enabled': False}
    when support is ambiguous in model metadata.
    """
    if not model_metadata or not isinstance(model_metadata, dict):
        return None

    reasoning_meta = model_metadata.get("reasoning")
    if not isinstance(reasoning_meta, dict):
        return None

    # If reasoning is mandatory, disabling effort is rejected
    if reasoning_meta.get("mandatory") is True:
        return None

    # Check confirmed supported efforts
    supported_efforts = reasoning_meta.get("supported_efforts")
    if isinstance(supported_efforts, list) and "none" in supported_efforts:
        return {"effort": "none"}

    # When supported_efforts is omitted or does not include 'none',
    # support is ambiguous; do not inject unverified reasoning keys.
    return None


def build_canary_request_params(
    api_key: str,
    model_id: str = EXACT_CANARY_MODEL_ID,
    model_metadata: Optional[Dict[str, Any]] = None,
    max_tokens: int = DEFAULT_CANARY_MAX_TOKENS,
    temperature: float = 0.1,
    app_url: str = "https://github.com/sandeep10704/conversa-ai",
    app_title: str = "Conversa AI Quality Canary",
) -> Dict[str, Any]:
    """Build the configuration dictionary for ChatOpenRouter.

    Guarantees:
    - Exact model ID matching EXACT_CANARY_MODEL_ID.
    - max_retries is strictly 0.
    - reasoning parameter is included only when confirmed supported by metadata.
    """
    if model_id != EXACT_CANARY_MODEL_ID:
        raise ValueError(
            f"Invalid canary model ID '{model_id}'. Must be exactly '{EXACT_CANARY_MODEL_ID}'."
        )

    params: Dict[str, Any] = {
        "model": model_id,
        "api_key": api_key,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "timeout": 60_000,
        "max_retries": 0,  # CRITICAL: zero automatic retries
        "app_url": app_url,
        "app_title": app_title,
    }

    reasoning_cfg = determine_reasoning_config(model_metadata)
    if reasoning_cfg is not None:
        params["reasoning"] = reasoning_cfg

    return params


def build_canary_prompt_messages(
    context_data: Dict[str, Any],
    system_prompt: str = CONCISE_SYSTEM_PROMPT,
) -> List[Tuple[str, str]]:
    """Build prompt messages (system, human) using structured context data."""
    return [
        ("system", system_prompt),
        ("human", json.dumps(context_data)),
    ]


async def execute_canary_parse(
    raw_response: Any,
    model_cls: type[BaseModel],
    stage: str = "generator",
    allow_repair: bool = False,
) -> Tuple[Optional[BaseModel], Optional[str]]:
    """Parse raw LLM output using existing parse_structured_output.

    Enforces allow_repair=False to guarantee zero secondary repair calls.
    Returns (validated_model, None) on success, or (None, error_message) on failure.
    """
    from app.core.parsing import StructuredOutputError, parse_structured_output

    try:
        parsed = await parse_structured_output(
            raw=raw_response,
            model_cls=model_cls,
            stage=stage,
            llm=None,
            allow_repair=allow_repair,
        )
        return parsed, None
    except StructuredOutputError as e:
        logger.warning("Canary response safely rejected: %s (stage=%s, category=%s)", e, stage, e.category)
        return None, f"Parsing failure [{e.category}]: {str(e)}"
    except Exception as e:
        logger.warning("Unexpected canary parsing exception: %s", e)
        return None, f"Unexpected parsing failure: {str(e)}"
