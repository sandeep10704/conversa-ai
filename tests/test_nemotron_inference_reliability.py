"""Offline regression tests for Nemotron inference reliability fix preparation.

Verifies:
1. Exact model ID enforcement (nvidia/nemotron-3.5-lightning:free).
2. Reasoning configuration is included only when confirmed supported by metadata.
3. max_retries is strictly zero.
4. No automatic parser repair occurs on malformed or empty output.
5. The concise prompt preserves all 6 required grounding, distinction, and repetition rules.
6. Empty and truncated model responses are safely rejected.
7. Existing valid tool-call and JSON responses still parse correctly.
8. Outbound network inference is strictly prohibited.
"""

from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage

from app.core.parsing import ParsingErrorCategory, StructuredOutputError, parse_structured_output
from app.schemas.agent import SuggestionItem, SuggestionSet
try:
    from canary_request_builder import (
        CONCISE_SYSTEM_PROMPT,
        DEFAULT_CANARY_MAX_TOKENS,
        EXACT_CANARY_MODEL_ID,
        build_canary_prompt_messages,
        build_canary_request_params,
        determine_reasoning_config,
        execute_canary_parse,
    )
except ImportError:
    from tests.canary_request_builder import (
        CONCISE_SYSTEM_PROMPT,
        DEFAULT_CANARY_MAX_TOKENS,
        EXACT_CANARY_MODEL_ID,
        build_canary_prompt_messages,
        build_canary_request_params,
        determine_reasoning_config,
        execute_canary_parse,
    )


@pytest.fixture(autouse=True)
def prohibit_outbound_network(monkeypatch):
    """Enforce zero outbound network calls during test execution."""
    import socket

    def blocked_connect(*args, **kwargs):
        raise RuntimeError("PROHIBITED: Outbound network call attempted during offline test!")

    monkeypatch.setattr(socket, "create_connection", blocked_connect)


# ---------------------------------------------------------------------------
# 1. Exact Model ID Enforcement
# ---------------------------------------------------------------------------

def test_request_builder_exact_model_id():
    """Verify request builder enforces EXACT_CANARY_MODEL_ID."""
    params = build_canary_request_params(api_key="test-secret-key")
    assert params["model"] == EXACT_CANARY_MODEL_ID
    assert params["model"] == "nvidia/nemotron-3.5-lightning:free"


def test_request_builder_rejects_non_canary_model():
    """Verify request builder raises ValueError if an unexpected model ID is passed."""
    with pytest.raises(ValueError, match="Invalid canary model ID"):
        build_canary_request_params(
            api_key="test-secret-key",
            model_id="openrouter/free",
        )


# ---------------------------------------------------------------------------
# 2. Reasoning Configuration Support Verification
# ---------------------------------------------------------------------------

def test_reasoning_config_when_metadata_missing():
    """When metadata is None, reasoning config must be omitted (unconfirmed)."""
    assert determine_reasoning_config(None) is None
    params = build_canary_request_params(api_key="test-key", model_metadata=None)
    assert "reasoning" not in params


def test_reasoning_config_for_actual_nemotron_free_metadata():
    """Actual OpenRouter catalog for nvidia/nemotron-3.5-lightning:free omits supported_efforts.

    Support for effort: 'none' is ambiguous/unconfirmed; reasoning must NOT be blindly injected.
    """
    actual_nemotron_metadata = {
        "id": "nvidia/nemotron-3.5-lightning:free",
        "supported_parameters": ["include_reasoning", "max_tokens", "reasoning", "temperature"],
        "reasoning": {
            "mandatory": False,
        },
    }
    assert determine_reasoning_config(actual_nemotron_metadata) is None

    params = build_canary_request_params(
        api_key="test-key",
        model_metadata=actual_nemotron_metadata,
    )
    assert "reasoning" not in params


def test_reasoning_config_when_none_is_explicitly_supported():
    """When metadata explicitly includes 'none' in supported_efforts, effort: 'none' is set."""
    confirmed_metadata = {
        "id": "test/confirmed-model",
        "reasoning": {
            "mandatory": False,
            "supported_efforts": ["high", "medium", "none"],
            "default_effort": "high",
        },
    }
    assert determine_reasoning_config(confirmed_metadata) == {"effort": "none"}

    params = build_canary_request_params(
        api_key="test-key",
        model_metadata=confirmed_metadata,
    )
    assert params["reasoning"] == {"effort": "none"}


def test_reasoning_config_when_mandatory_is_true():
    """When reasoning is mandatory, effort: 'none' is rejected by OpenRouter; must return None."""
    mandatory_metadata = {
        "id": "test/mandatory-reasoning-model",
        "reasoning": {
            "mandatory": True,
            "supported_efforts": ["high", "medium"],
        },
    }
    assert determine_reasoning_config(mandatory_metadata) is None


def test_reasoning_config_when_none_not_in_supported_efforts():
    """When supported_efforts does not include 'none', effort: 'none' cannot be safely sent."""
    restricted_metadata = {
        "id": "test/restricted-efforts-model",
        "reasoning": {
            "mandatory": False,
            "supported_efforts": ["high", "medium", "low"],
        },
    }
    assert determine_reasoning_config(restricted_metadata) is None


# ---------------------------------------------------------------------------
# 3. max_retries Remains Zero
# ---------------------------------------------------------------------------

def test_max_retries_is_strictly_zero():
    """Verify max_retries is strictly 0 to prevent runaway automatic retries."""
    params = build_canary_request_params(api_key="test-key")
    assert params["max_retries"] == 0
    assert params["temperature"] == 0.1
    assert params["max_tokens"] == DEFAULT_CANARY_MAX_TOKENS


# ---------------------------------------------------------------------------
# 4. No Automatic Parser Repair Occurs
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_no_parser_repair_attempted():
    """Verify execute_canary_parse enforces allow_repair=False and does not call repair."""
    malformed_raw = AIMessage(content="This is plain text with no JSON and no tool call.")

    with patch("app.core.parsing.execute_repair_attempt") as mock_repair:
        result, error = await execute_canary_parse(
            raw_response=malformed_raw,
            model_cls=SuggestionSet,
            stage="generator",
            allow_repair=False,
        )
        assert result is None
        assert error is not None
        assert "Parsing failure" in error
        # Verify repair was NEVER called
        mock_repair.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Shortened Prompt Preserves All Grounding Rules
# ---------------------------------------------------------------------------

def test_concise_prompt_preserves_all_grounding_rules():
    """Verify the concise prompt retains all 6 core quality and factual grounding criteria."""
    prompt = CONCISE_SYSTEM_PROMPT

    # 1. Spoken style / natural contribution
    assert "Spoken Style" in prompt or "speaking aloud" in prompt
    assert "natural" in prompt.lower()

    # 2. Factual grounding
    assert "Factual Grounding" in prompt
    assert "stated in the conversation" in prompt.lower() or "facts stated" in prompt.lower()

    # 3. Participant-reported figures treated as observations/estimates
    assert "Silero VAD" in prompt
    assert "Whisper Tiny" in prompt
    assert "observations" in prompt.lower() or "estimates" in prompt.lower()
    assert "verified" in prompt.lower() or "benchmarks" in prompt.lower()

    # 4. VAD chunk processing vs silence threshold distinction
    assert "Latency & Silence Distinction" in prompt or "silence" in prompt.lower()
    assert "VAD chunk processing" in prompt or "processing latency" in prompt
    assert "silence thresholds" in prompt or "turn-taking" in prompt

    # 5. Unsupported numerical recommendation protection (profiling / calibrating)
    assert "No Invented" in prompt or "invent" in prompt.lower()
    assert "measuring" in prompt.lower() or "calibrating" in prompt.lower() or "profiling" in prompt.lower()

    # 6. Anti-repetition awareness
    assert "Anti-Repetition" in prompt or "previous_suggestions" in prompt

    # 7. Single suggestion requirement & JSON schema
    assert "ONE concise spoken suggestion" in prompt
    assert '"suggestions"' in prompt
    assert '"response_type"' in prompt
    assert '"text"' in prompt
    assert '"rationale"' in prompt

    # 8. Length reduction check: concise prompt must be < 200 words (vs 336 words in previous canary)
    word_count = len(prompt.split())
    assert word_count < 200, f"Prompt length ({word_count} words) is too long; should be < 200 words"


# ---------------------------------------------------------------------------
# 6. Empty and Truncated Responses Are Safely Rejected
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_empty_content_response_rejected():
    """Empty content (like the failed canary) must be rejected with EMPTY_RESPONSE error."""
    empty_raw = AIMessage(content="", tool_calls=[])

    # Direct parser check
    with pytest.raises(StructuredOutputError) as exc_info:
        await parse_structured_output(
            raw=empty_raw,
            model_cls=SuggestionSet,
            stage="generator",
            allow_repair=False,
        )
    assert exc_info.value.category == ParsingErrorCategory.EMPTY_RESPONSE

    # Canary runner check
    result, error = await execute_canary_parse(
        raw_response=empty_raw,
        model_cls=SuggestionSet,
        stage="generator",
        allow_repair=False,
    )
    assert result is None
    assert "empty_response" in error


@pytest.mark.anyio
async def test_truncated_json_response_rejected():
    """Truncated JSON (cut off by token limit) must be rejected with JSON_PARSE_ERROR."""
    truncated_raw = AIMessage(
        content='{"suggestions": [{"response_type": "direct_answer", "text": "Wait for Silero VAD',
        tool_calls=[],
    )

    # Direct parser check
    with pytest.raises(StructuredOutputError) as exc_info:
        await parse_structured_output(
            raw=truncated_raw,
            model_cls=SuggestionSet,
            stage="generator",
            allow_repair=False,
        )
    assert exc_info.value.category == ParsingErrorCategory.JSON_PARSE_ERROR

    # Canary runner check
    result, error = await execute_canary_parse(
        raw_response=truncated_raw,
        model_cls=SuggestionSet,
        stage="generator",
        allow_repair=False,
    )
    assert result is None
    assert "json_parse_error" in error


# ---------------------------------------------------------------------------
# 7. Valid Tool-Call and JSON Responses Still Parse Correctly
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_valid_json_content_parses_cleanly():
    """Valid raw JSON content parses cleanly into SuggestionSet."""
    valid_json = json.dumps({
        "suggestions": [
            {
                "response_type": "direct_answer",
                "text": "We should empirically profile the silence threshold before triggering TTS.",
                "rationale": "Calibrating silence duration prevents interrupting the user without guessing.",
                "follow_up_question": None,
            }
        ]
    })
    raw = AIMessage(content=valid_json, tool_calls=[])

    result, error = await execute_canary_parse(
        raw_response=raw,
        model_cls=SuggestionSet,
        stage="generator",
        allow_repair=False,
    )
    assert error is None
    assert isinstance(result, SuggestionSet)
    assert len(result.suggestions) == 1
    assert result.suggestions[0].response_type == "direct_answer"
    assert "empirically profile" in result.suggestions[0].text


@pytest.mark.anyio
async def test_valid_tool_call_parses_cleanly():
    """Valid native tool call parses cleanly into SuggestionSet."""
    raw = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "SuggestionSet",
                "args": {
                    "suggestions": [
                        {
                            "response_type": "direct_answer",
                            "text": "Measure the pipeline latency end-to-end under real audio conditions.",
                            "rationale": "Provides empirical baseline for turn-taking boundaries.",
                            "follow_up_question": None,
                        }
                    ]
                },
                "id": "call_12345",
            }
        ],
    )

    result, error = await execute_canary_parse(
        raw_response=raw,
        model_cls=SuggestionSet,
        stage="generator",
        allow_repair=False,
    )
    assert error is None
    assert isinstance(result, SuggestionSet)
    assert len(result.suggestions) == 1
    assert result.suggestions[0].response_type == "direct_answer"
    assert "Measure the pipeline latency" in result.suggestions[0].text
