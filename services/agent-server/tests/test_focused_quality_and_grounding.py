import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.anyio

from app.agent.graph import get_agent_graph
from app.agent.nodes.validator import (
    _containment_similarity,
    _extract_metric_quantities,
    _extract_numeric_tokens,
    _jaccard_similarity,
    _tokenize_content,
    validator_node,
)
from app.agent.state import AgentState
from app.schemas.agent import (
    DiscussionAnalysis,
    RelevanceAssessment,
    SuggestionItem,
    SuggestionSet,
)


@pytest.fixture
def mock_openrouter_zero_calls():
    """Safety guard fixture verifying zero external OpenRouter API calls are made."""
    with patch("httpx.AsyncClient.post") as mock_post, \
         patch("httpx.AsyncClient.send") as mock_send, \
         patch("requests.post") as mock_req_post:
        yield {
            "mock_post": mock_post,
            "mock_send": mock_send,
            "mock_req_post": mock_req_post,
        }
        assert mock_post.call_count == 0, "Security violation: httpx.post called during offline evaluation!"
        assert mock_send.call_count == 0, "Security violation: httpx.send called during offline evaluation!"
        assert mock_req_post.call_count == 0, "Security violation: requests.post called during offline evaluation!"


class TestDeterministicValidator:
    """Deterministic validation tests for all required checks."""

    async def test_empty_and_short_suggestions_rejected(self):
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [{"sequence_number": 1, "text": "Testing suggestions."}],
            "suggestions": [
                {"text": "", "rationale": "Empty", "response_type": "supporting_point"},
                {"text": "Short", "rationale": "Too short", "response_type": "supporting_point"},
            ],
            "previous_suggestions": [],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert len(result["suggestions"]) == 0
        assert any("too short or empty" in w.lower() for w in result["validation_warnings"])

    async def test_excessive_length_rejected(self):
        long_text = "This suggestion is way too long. " * 40  # > 1000 chars
        assert len(long_text) > 1000
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [{"sequence_number": 1, "text": "Testing suggestions."}],
            "suggestions": [
                {"text": long_text, "rationale": "Long text test", "response_type": "supporting_point"},
            ],
            "previous_suggestions": [],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert any("exceeded the length limit" in w.lower() for w in result["validation_warnings"])

    async def test_missing_rationale_rejected(self):
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [{"sequence_number": 1, "text": "Testing suggestions."}],
            "suggestions": [
                {"text": "Valid suggestion text here.", "rationale": "", "response_type": "supporting_point"},
            ],
            "previous_suggestions": [],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert any("no relevance explanation" in w.lower() for w in result["validation_warnings"])

    async def test_invalid_response_type_rejected(self):
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [{"sequence_number": 1, "text": "Testing suggestions."}],
            "suggestions": [
                {
                    "text": "Valid length and substantive content.",
                    "rationale": "Valid rationale.",
                    "response_type": "hallucinated_invalid_type",
                },
            ],
            "previous_suggestions": [],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert any("invalid response_type" in w.lower() for w in result["validation_warnings"])

    async def test_generic_platitude_rejected(self):
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [{"sequence_number": 1, "text": "Testing suggestions."}],
            "suggestions": [
                {
                    "text": "Consider reviewing the asynchronous event pipeline latency metrics.",
                    "rationale": "Monitoring metrics.",
                    "response_type": "supporting_point",
                },
                {
                    "text": "We should review latency metrics.",
                    "rationale": "General review.",
                    "response_type": "supporting_point",
                },
            ],
            "previous_suggestions": [],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert any("generic platitude rejected" in w.lower() for w in result["validation_warnings"])

    async def test_repetition_exact_and_idea_overlap_rejected(self):
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [{"sequence_number": 1, "text": "The audio gateway received a 16 kHz stream with errors."}],
            "previous_suggestions": [
                {
                    "suggestion_text": "We should enforce 16 kHz mono 16-bit PCM at the Audio Gateway to eliminate client-side transcoding issues.",
                    "rationale": "Prevents downstream transcription failures.",
                }
            ],
            "suggestions": [
                # Similar idea with different phrasing
                {
                    "text": "We must require 16 kHz mono 16-bit PCM at the Audio Gateway to eliminate client-side transcoding problems.",
                    "rationale": "Audio gateway compatibility.",
                    "response_type": "supporting_point",
                },
            ],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert any("repeated suggestion" in w.lower() for w in result["validation_warnings"])

    async def test_legitimate_context_numbers_allowed(self):
        # Conversation explicitly mentions 50 ms and 180 ms
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [
                {"sequence_number": 1, "text": "Currently Silero VAD takes 50 ms and Whisper Tiny takes 180 ms."},
            ],
            "previous_suggestions": [],
            "suggestions": [
                {
                    "text": "Given the 50 ms VAD and 180 ms Whisper times, we should profile our silence threshold.",
                    "rationale": "Builds directly on participant-stated metrics.",
                    "response_type": "supporting_point",
                }
            ],
        }
        result = await validator_node(state)
        assert result["status"] == "suggestions_ready"
        assert len(result["suggestions"]) == 1
        assert result["suggestions"][0]["text"].startswith("Given the 50 ms VAD")

    async def test_unsupported_metrics_and_invented_thresholds_rejected(self):
        # Conversation only mentions 50 ms and 180 ms
        state: AgentState = {
            "session_id": "test-session-1",
            "messages": [
                {"sequence_number": 1, "text": "Currently Silero VAD takes 50 ms and Whisper Tiny takes 180 ms."},
            ],
            "previous_suggestions": [],
            "suggestions": [
                {
                    # 270 ms and 100 ms are NOT in conversation context
                    "text": "Silero VAD's 50 ms and Whisper Tiny's 180 ms leave about 270 ms for the LLM; add a brief safety margin of ~100 ms to ensure smooth delivery.",
                    "rationale": "Latency calculation.",
                    "response_type": "supporting_point",
                }
            ],
        }
        result = await validator_node(state)
        assert result["status"] == "no_suggestion"
        assert any("unsupported numeric recommendation" in w.lower() for w in result["validation_warnings"])
        assert any("safety margin" in w.lower() for w in result["validation_warnings"])


class TestSixFocusedScenarios:
    """The six focused scenarios evaluated through deterministic state and mocked LLM nodes."""

    async def test_scenario_1_direct_technical_question_with_context(self, mock_openrouter_zero_calls):
        """Scenario 1: Direct technical question with enough context.
        Should generate a direct, grounded answer without invented numbers.
        """
        state: AgentState = {
            "session_id": "scenario-1",
            "situation": "Conversa AI latency optimization discussion",
            "objective": "Help team resolve database outbox polling latency",
            "communication_goals": ["Provide concrete advice on reducing outbox latency below 100 ms"],
            "messages": [
                {"sequence_number": 1, "text": "We are seeing around 200 ms latency between writing an outbox event and the publisher picking it up."},
                {"sequence_number": 2, "text": "How can we configure outbox delivery to keep latency under 100 ms without polling busy loops?"},
            ],
            "previous_suggestions": [],
            "suggestions": [
                {
                    "response_type": "direct_answer",
                    "text": "We can switch from periodic polling to PostgreSQL LISTEN/NOTIFY, triggering immediate notification upon commit without busy looping.",
                    "rationale": "Directly answers how to achieve sub-100 ms outbox dispatch without CPU polling spikes.",
                }
            ],
        }
        res = await validator_node(state)
        assert res["status"] == "suggestions_ready"
        assert len(res["suggestions"]) == 1
        suggestion = res["suggestions"][0]
        assert suggestion["response_type"] == "direct_answer"
        assert "LISTEN/NOTIFY" in suggestion["text"]
        assert len(res["validation_warnings"]) == 0

    async def test_scenario_2_topic_discussion_with_unresolved_decision(self, mock_openrouter_zero_calls):
        """Scenario 2: Topic discussion with an unresolved engineering decision (no direct question to user).
        Should generate a grounded supporting suggestion recommending empirical measurement.
        """
        state: AgentState = {
            "session_id": "scenario-2",
            "situation": "Speech recognition architecture meeting",
            "objective": "Help decide between Whisper Tiny and Base models",
            "communication_goals": ["Advocate for measuring latency before switching models"],
            "messages": [
                {"sequence_number": 1, "text": "Whisper Tiny gives us 180 ms latency, but word error rate is noticeable on technical jargon."},
                {"sequence_number": 2, "text": "Whisper Base would fix transcription errors, but we don't know the exact latency overhead on our CPU instances."},
            ],
            "previous_suggestions": [],
            "suggestions": [
                {
                    "response_type": "supporting_point",
                    "text": "We should benchmark Whisper Base against our actual meeting audio to measure the exact latency trade-off before committing to the switch.",
                    "rationale": "Recommends empirical testing for the unknown Whisper Base latency parameter.",
                }
            ],
        }
        res = await validator_node(state)
        assert res["status"] == "suggestions_ready"
        assert len(res["suggestions"]) == 1
        assert "benchmark Whisper Base" in res["suggestions"][0]["text"]

    async def test_scenario_3_completely_unrelated_conversation(self, mock_openrouter_zero_calls):
        """Scenario 3: Completely unrelated conversation (social small talk about lunch).
        Relevance assessment must return should_suggest=False and suppress suggestions.
        """
        # Test full graph conditional routing
        state: AgentState = {
            "session_id": "scenario-3",
            "situation": "Conversa AI technical architecture review",
            "objective": "Optimize real-time audio pipeline latency",
            "communication_goals": ["Reduce end-to-end pipeline latency"],
            "messages": [
                {"sequence_number": 1, "text": "Hey team, where are we heading for lunch today?"},
                {"sequence_number": 2, "text": "Tacos or pizza sounds good to me. Let's head out around noon."},
            ],
            "previous_suggestions": [],
        }

        # Mock understanding and relevance nodes to simulate LLM evaluating unrelated chitchat
        analysis_mock = DiscussionAnalysis(
            current_topic="Team lunch plans",
            summary="Discussion regarding lunch location options.",
            key_points=["Lunch options include tacos or pizza"],
            unresolved_questions=["Where to eat lunch"],
            detected_intent="statement",
            intent_evidence="head out around noon",
        )
        relevance_mock = RelevanceAssessment(
            should_suggest=False,
            confidence=0.98,
            reason="Discussion is social small talk regarding lunch, entirely unrelated to audio pipeline latency.",
            response_opportunity="",
        )

        with patch("app.agent.nodes.understanding.get_llm") as mock_get_llm_u, \
             patch("app.agent.nodes.relevance.get_llm") as mock_get_llm_r:

            mock_llm_u = MagicMock()
            mock_llm_u.bind_tools.return_value.ainvoke = AsyncMock(return_value=MagicMock(tool_calls=[{
                "name": "DiscussionAnalysis",
                "args": analysis_mock.model_dump(),
            }]))
            mock_get_llm_u.return_value = mock_llm_u

            mock_llm_r = MagicMock()
            mock_llm_r.bind_tools.return_value.ainvoke = AsyncMock(return_value=MagicMock(tool_calls=[{
                "name": "RelevanceAssessment",
                "args": relevance_mock.model_dump(),
            }]))
            mock_get_llm_r.return_value = mock_llm_r

            graph = get_agent_graph()
            result = await graph.ainvoke(state)

            assert result.get("should_suggest") is False
            assert len(result.get("suggestions", [])) == 0
            # Graph routed to finish/END without calling generator
            assert result.get("stage_reached") in ["relevance", "understanding"]

    async def test_scenario_4_all_discussion_goals_already_addressed(self, mock_openrouter_zero_calls):
        """Scenario 4: All discussion goals already addressed.
        Relevance assessment must detect that user goals are satisfied, suppressing suggestions.
        """
        state: AgentState = {
            "session_id": "scenario-4",
            "situation": "Kafka partition key assignment review",
            "objective": "Ensure transcript ordering per session",
            "communication_goals": ["Ensure session_id is used as Kafka partition key for transcripts"],
            "messages": [
                {"sequence_number": 1, "text": "We need in-order delivery for transcript events in each session."},
                {"sequence_number": 2, "text": "We have set session_id as the partition key and verified in-order delivery across all partitions."},
                {"sequence_number": 3, "text": "Confirmed, all messages arrive strictly in order now. That task is complete."},
            ],
            "previous_suggestions": [
                {
                    "suggestion_text": "We should partition Kafka transcript events by session_id to maintain sequence order.",
                    "rationale": "Guarantees in-order delivery per session.",
                }
            ],
        }

        analysis_mock = DiscussionAnalysis(
            current_topic="Kafka partition key verification",
            summary="Team confirmed partition by session_id is deployed and verified in-order delivery.",
            key_points=["Partition by session_id is working and tested"],
            unresolved_questions=[],
            detected_intent="statement",
            intent_evidence="That task is complete",
        )
        relevance_mock = RelevanceAssessment(
            should_suggest=False,
            confidence=0.95,
            reason="Communication goal regarding session_id partition key has already been fully satisfied in the discussion.",
            response_opportunity="",
        )

        with patch("app.agent.nodes.understanding.get_llm") as mock_get_llm_u, \
             patch("app.agent.nodes.relevance.get_llm") as mock_get_llm_r:

            mock_llm_u = MagicMock()
            mock_llm_u.bind_tools.return_value.ainvoke = AsyncMock(return_value=MagicMock(tool_calls=[{
                "name": "DiscussionAnalysis",
                "args": analysis_mock.model_dump(),
            }]))
            mock_get_llm_u.return_value = mock_llm_u

            mock_llm_r = MagicMock()
            mock_llm_r.bind_tools.return_value.ainvoke = AsyncMock(return_value=MagicMock(tool_calls=[{
                "name": "RelevanceAssessment",
                "args": relevance_mock.model_dump(),
            }]))
            mock_get_llm_r.return_value = mock_llm_r

            graph = get_agent_graph()
            result = await graph.ainvoke(state)

            assert result.get("should_suggest") is False
            assert len(result.get("suggestions", [])) == 0

    async def test_scenario_5_previous_suggestion_repeated_in_later_response(self, mock_openrouter_zero_calls):
        """Scenario 5: Previous suggestion repeated in a later model response.
        Validator must detect token/idea overlap (> 0.50 Jaccard) and reject the repetition.
        """
        previous_text = "We should enforce 16 kHz mono 16-bit PCM at the Audio Gateway to eliminate client-side transcoding issues."
        repeated_candidate = "We should require 16 kHz mono 16-bit PCM format on the Audio Gateway WebSocket to stop transcoding errors."

        tokens_prev = _tokenize_content(previous_text)
        tokens_cand = _tokenize_content(repeated_candidate)
        sim = _jaccard_similarity(tokens_prev, tokens_cand)
        containment = _containment_similarity(tokens_prev, tokens_cand)
        assert sim >= 0.40 or containment >= 0.50, f"Expected similarity >= 0.40 or containment >= 0.50, got sim={sim:.2f}, cont={containment:.2f}"

        state: AgentState = {
            "session_id": "scenario-5",
            "messages": [
                {"sequence_number": 1, "text": "The gateway encountered a non-standard audio stream today instead of the expected 16 kHz."},
            ],
            "previous_suggestions": [
                {
                    "suggestion_text": previous_text,
                    "rationale": "Prevents transcription failures.",
                }
            ],
            "suggestions": [
                {
                    "response_type": "supporting_point",
                    "text": repeated_candidate,
                    "rationale": "Ensure PCM audio format compatibility.",
                }
            ],
        }

        res = await validator_node(state)
        assert res["status"] == "no_suggestion"
        assert len(res["suggestions"]) == 0
        assert any("repeated suggestion" in w.lower() for w in res["validation_warnings"])

    async def test_scenario_6_regression_unsupported_vad_timing_conflation(self, mock_openrouter_zero_calls):
        """Scenario 6: Technical recommendation requiring measurements that are not available.
        Regression test:
        - Flawed candidate introducing invented numbers (270 ms, 100 ms) and safety margin is REJECTED.
        - Grounded candidate recommending measurement and qualifying participant estimates is ACCEPTED.
        """
        # Context from live microphone session
        messages = [
            {"sequence_number": 1, "text": "Many need to discuss our response generation latency in the live audio pipeline."},
            {"sequence_number": 2, "text": "Currently, solar V8 data takes about 50 million seconds and whisper tiny text, around 180 milliseconds."},
        ]

        # 6A: Flawed suggestion from Experiment 2 (contains 270 ms, 100 ms safety margin)
        flawed_state: AgentState = {
            "session_id": "scenario-6-flawed",
            "messages": messages,
            "previous_suggestions": [],
            "suggestions": [
                {
                    "response_type": "supporting_point",
                    "text": "Silero VAD's 50 ms and Whisper Tiny's 180 ms leave about 270 ms for the LLM; add a brief safety margin of ~100 ms to ensure smooth delivery.",
                    "rationale": "Timing budget calculation.",
                }
            ],
        }
        flawed_res = await validator_node(flawed_state)
        assert flawed_res["status"] == "no_suggestion"
        assert len(flawed_res["suggestions"]) == 0
        assert any("unsupported numeric recommendation" in w.lower() for w in flawed_res["validation_warnings"])
        assert any("safety margin" in w.lower() for w in flawed_res["validation_warnings"])

        # 6B: Grounded suggestion (recommends empirical calibration of silence threshold, uses only stated numbers)
        grounded_state: AgentState = {
            "session_id": "scenario-6-grounded",
            "messages": messages,
            "previous_suggestions": [],
            "suggestions": [
                {
                    "response_type": "supporting_point",
                    "text": "The 50 ms VAD and 180 ms Whisper times are participant estimates. We should measure the end-of-speech silence detection threshold before fixing an LLM latency budget.",
                    "rationale": "Clarifies participant estimates and recommends measuring silence detection threshold rather than inventing safety margins.",
                }
            ],
        }
        grounded_res = await validator_node(grounded_state)
        assert grounded_res["status"] == "suggestions_ready"
        assert len(grounded_res["suggestions"]) == 1
        accepted_text = grounded_res["suggestions"][0]["text"]
        assert "participant estimates" in accepted_text
        assert "measure the end-of-speech silence detection threshold" in accepted_text
