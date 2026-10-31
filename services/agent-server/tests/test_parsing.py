"""Unit tests for the robust structured output parsing utility."""

import json
from unittest.mock import AsyncMock, MagicMock
import pytest
from pydantic import BaseModel, Field

from app.core.parsing import (
    ParsingErrorCategory,
    StructuredOutputError,
    extract_balanced_json,
    extract_raw_json,
    normalize_payload_for_schema,
    parse_structured_output,
)
from app.schemas.agent import DiscussionAnalysis, RelevanceAssessment, SuggestionSet


class SimpleModel(BaseModel):
    name: str
    count: int = Field(default=0)


class TestParsingUtility:
    """Test JSON extraction, normalization, and Pydantic validation."""

    def test_extract_balanced_json_simple(self):
        text = 'Prose before {"name": "Alice", "count": 5} prose after.'
        result = extract_balanced_json(text)
        assert result == '{"name": "Alice", "count": 5}'

    def test_extract_balanced_json_nested(self):
        text = 'Prefix {"outer": {"inner": [1, 2, {"a": "b"}]}} Suffix'
        result = extract_balanced_json(text)
        assert result == '{"outer": {"inner": [1, 2, {"a": "b"}]}}'

    def test_extract_raw_json_markdown_fence(self):
        text = """Here is the result:
```json
{
  "name": "Bob",
  "count": 10
}
```
Hope this helps!"""
        result = extract_raw_json(text)
        assert result == {"name": "Bob", "count": 10}

    def test_normalize_payload_suggestions_as_list(self):
        raw_list = [
            {"suggestion": "Check logs.", "type": "supporting_point"},
            {"text": "Restart service.", "response_type": "direct_answer", "rationale": "Clear crash."},
        ]
        norm = normalize_payload_for_schema(raw_list, SuggestionSet)
        assert "suggestions" in norm
        assert len(norm["suggestions"]) == 2
        assert norm["suggestions"][0]["text"] == "Check logs."
        assert norm["suggestions"][0]["response_type"] == "supporting_point"
        assert norm["suggestions"][1]["text"] == "Restart service."

    @pytest.mark.anyio
    async def test_parse_tool_call_response(self):
        msg = MagicMock()
        msg.tool_calls = [{"args": {"name": "Charlie", "count": 42}}]
        msg.content = ""

        result = await parse_structured_output(msg, SimpleModel, stage="test")
        assert result.name == "Charlie"
        assert result.count == 42

    @pytest.mark.anyio
    async def test_parse_markdown_fence_content(self):
        msg = MagicMock()
        msg.tool_calls = []
        msg.invalid_tool_calls = []
        msg.content = '```json\n{"current_topic": "Auth", "summary": "Discussion on auth", "detected_intent": "statement"}\n```'

        result = await parse_structured_output(msg, DiscussionAnalysis, stage="understanding")
        assert result.current_topic == "Auth"
        assert result.summary == "Discussion on auth"
        assert result.detected_intent == "statement"

    @pytest.mark.anyio
    async def test_parse_validation_error_raises_structured_output_error(self):
        msg = MagicMock()
        msg.tool_calls = []
        msg.invalid_tool_calls = []
        # Missing required field 'current_topic'
        msg.content = '{"summary": "No topic here", "detected_intent": "statement"}'

        with pytest.raises(StructuredOutputError) as exc_info:
            await parse_structured_output(msg, DiscussionAnalysis, stage="understanding", allow_repair=False)

        err = exc_info.value
        assert err.stage == "understanding"
        assert err.category == ParsingErrorCategory.SCHEMA_VALIDATION_ERROR
        assert any("current_topic" in e for e in err.validation_errors)

    @pytest.mark.anyio
    async def test_controlled_repair_attempt_success(self):
        # Initial output is malformed
        bad_msg = MagicMock()
        bad_msg.tool_calls = []
        bad_msg.invalid_tool_calls = []
        bad_msg.content = "Not valid JSON at all!"

        # Mock repair LLM returning valid JSON
        mock_llm = MagicMock()
        repaired_msg = MagicMock()
        repaired_msg.tool_calls = []
        repaired_msg.invalid_tool_calls = []
        repaired_msg.content = '{"name": "Repaired", "count": 99}'
        mock_llm.ainvoke = AsyncMock(return_value=repaired_msg)

        result = await parse_structured_output(
            bad_msg, SimpleModel, stage="test_repair", llm=mock_llm, allow_repair=True
        )
        assert result.name == "Repaired"
        assert result.count == 99
        mock_llm.ainvoke.assert_called_once()
