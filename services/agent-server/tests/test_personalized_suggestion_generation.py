"""Focused deterministic tests for Milestone 1: Personalized Suggestion Generation.

Validates:
1. Relevant discussion creates a tailored suggestion.
2. Technical discussion without direct name mention.
3. No fabricated personal experience.
4. Participant constraints are respected.
5. Irrelevant profile information is excluded.
6. Missing context uses general generation.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

from app.agent.compact_context import select_compact_participant_context
from app.agent.nodes.generator import SYSTEM_PROMPT, generator_node
from app.agent.state import AgentState
from app.schemas.agent import SuggestionItem, SuggestionSet


class TestPersonalizedSuggestionGeneration:
    """Six deterministic tests for Milestone 1: Personalized Suggestion Generation."""

    @pytest.mark.anyio
    async def test_01_relevant_discussion_creates_tailored_suggestion(self):
        """1. Relevant discussion creates a tailored suggestion matching role & expertise."""
        state: AgentState = {
            "session_id": "test-tailored-01",
            "situation": "Platform Architecture Sync",
            "objective": "Resolve streaming bottlenecks",
            "communication_goals": ["Provide backend streaming insights"],
            "constraints": ["Keep suggestions concise"],
            "response_preferences": {"tone": "technical", "detail_level": "concise"},
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "responsibilities": "Kafka event streaming and partition management",
                "expertise_keywords": ["Kafka", "PostgreSQL", "Distributed Systems"],
                "session_goals": "Advise on consumer lag mitigation",
                "constraints": "Keep suggestions concise",
            },
            "current_topic": "Kafka consumer lag",
            "conversation_summary": "Team is experiencing severe consumer lag on audio transcript topics.",
            "detected_intent": "question",
            "messages": [
                {"sequence_number": 1, "text": "We have substantial consumer lag building up on the Kafka topics."},
                {"sequence_number": 2, "text": "How should we mitigate this without dropping messages?"},
            ],
            "previous_suggestions": [],
            "relevance": {
                "should_suggest": True,
                "confidence": 0.95,
                "reason": "Kafka consumer lag directly matches Sandeep's streaming expertise.",
                "response_opportunity": "Suggest increasing Kafka partition count and matching consumer concurrency.",
            },
        }

        mock_sugg_set = SuggestionSet(
            suggestions=[
                SuggestionItem(
                    response_type="direct_answer",
                    text="We should increase topic partitions from 3 to 12 and scale our consumer group concurrency to match.",
                    rationale="Directly addresses Kafka consumer throughput bottlenecks based on partition concurrency.",
                    follow_up_question="What is our current partition key distribution?",
                )
            ]
        )

        with patch("app.agent.nodes.generator.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "SuggestionSet", "args": mock_sugg_set.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await generator_node(state)

            # Assert generator LLM was called with compact context
            call_args = mock_llm.bind_tools.return_value.ainvoke.call_args[0][0]
            human_payload = json.loads(call_args[1][1])
            assert "participant_context" in human_payload
            compact = human_payload["participant_context"]
            assert compact["role"] == "Backend Developer"
            assert "Kafka" in compact["expertise_keywords"]
            assert compact["session_goals"] == "Advise on consumer lag mitigation"

            # Assert tailored suggestion output
            assert len(result["suggestions"]) == 1
            assert "partitions" in result["suggestions"][0]["text"]
            assert result["suggestions"][0]["response_type"] == "direct_answer"

    @pytest.mark.anyio
    async def test_02_technical_discussion_without_direct_name_mention(self):
        """2. Technical discussion without direct name mention grounds suggestion in expertise."""
        state: AgentState = {
            "session_id": "test-no-name-02",
            "situation": "Database Performance Meeting",
            "objective": "Identify database query latency sources",
            "communication_goals": ["Propose database optimization"],
            "constraints": [],
            "response_preferences": {},
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "responsibilities": "PostgreSQL database optimization",
                "expertise_keywords": ["PostgreSQL", "connection pooling"],
            },
            "current_topic": "PostgreSQL query latency",
            "conversation_summary": "Latency on message retrieval queries spiked to 450ms.",
            "detected_intent": "statement",
            "messages": [
                {"sequence_number": 1, "text": "Our p99 query latency on PostgreSQL has jumped to 450 milliseconds."},
                {"sequence_number": 2, "text": "The message table queries seem to be the primary culprit."},
            ],
            "previous_suggestions": [],
            "relevance": {
                "should_suggest": True,
                "confidence": 0.88,
                "reason": "Matches PostgreSQL expertise without explicit name invocation.",
                "response_opportunity": "Propose verifying composite index on (session_id, sequence_number).",
            },
        }

        mock_sugg_set = SuggestionSet(
            suggestions=[
                SuggestionItem(
                    response_type="supporting_point",
                    text="We should check if the index on session_id and sequence_number is being used, or if Postgres is executing a sequential scan.",
                    rationale="Grounded technical suggestion addressing query latency root cause without requiring a name callout.",
                )
            ]
        )

        with patch("app.agent.nodes.generator.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "SuggestionSet", "args": mock_sugg_set.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await generator_node(state)

            call_args = mock_llm.bind_tools.return_value.ainvoke.call_args[0][0]
            human_payload = json.loads(call_args[1][1])
            compact = human_payload["participant_context"]
            assert compact["role"] == "Backend Developer"
            assert "PostgreSQL" in compact["expertise_keywords"]

            # Suggestion is technically grounded
            assert "index" in result["suggestions"][0]["text"]

    @pytest.mark.anyio
    async def test_03_no_fabricated_personal_experience(self):
        """3. No fabricated personal experience: Prompt forbids inventing past employers or degrees."""
        # Verify prompt rules explicitly prohibit credential and past experience fabrication
        assert "without ever fabricating personal experience" in SYSTEM_PROMPT
        assert 'Never say "In my experience at [Company]..."' in SYSTEM_PROMPT

        state: AgentState = {
            "session_id": "test-no-fabrication-03",
            "situation": "Architecture Review",
            "objective": "Discuss microservices",
            "communication_goals": ["Provide technical perspective"],
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "expertise_keywords": ["Go", "Kafka"],
            },
            "current_topic": "Service decomposition",
            "conversation_summary": "Discussion on whether to split services.",
            "detected_intent": "question",
            "messages": [
                {"sequence_number": 1, "text": "What are the trade-offs of extracting the audio gateway into a separate service?"}
            ],
            "previous_suggestions": [],
        }

        mock_sugg_set = SuggestionSet(
            suggestions=[
                SuggestionItem(
                    response_type="direct_answer",
                    text="Separating the gateway isolates audio WebSocket connections from agent API load, but adds network hop latency.",
                    rationale="Factual technical trade-off without inventing past companies or employment claims.",
                )
            ]
        )

        with patch("app.agent.nodes.generator.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "SuggestionSet", "args": mock_sugg_set.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await generator_node(state)
            sugg_text = result["suggestions"][0]["text"]

            # Assert no fabricated credential or past employer phrasing
            assert "In my 10 years" not in sugg_text
            assert "When I worked at" not in sugg_text
            assert "At my previous company" not in sugg_text

    @pytest.mark.anyio
    async def test_04_participant_constraints_are_respected(self):
        """4. Participant constraints are strictly respected in generated suggestions."""
        state: AgentState = {
            "session_id": "test-constraints-04",
            "situation": "Sprint Planning",
            "objective": "Finalize commitments",
            "communication_goals": ["Avoid unrealistic timeline commitments"],
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "expertise_keywords": ["Kafka", "Audio Pipeline"],
                "constraints": "Do not commit to delivery deadlines or completion dates without prior estimation",
            },
            "current_topic": "Kafka deployment timeline",
            "conversation_summary": "Lead asks if Kafka migration can finish by tomorrow noon.",
            "detected_intent": "question",
            "messages": [
                {"sequence_number": 1, "text": "Can we guarantee the Kafka migration will be fully deployed by tomorrow noon?"}
            ],
            "previous_suggestions": [],
        }

        # Mock generator properly respecting the constraint
        mock_sugg_set = SuggestionSet(
            suggestions=[
                SuggestionItem(
                    response_type="direct_answer",
                    text="We need to benchmark partition rebalancing under peak load first before committing to a deployment schedule.",
                    rationale="Avoids committing to the proposed deadline in strict adherence to participant constraint.",
                )
            ]
        )

        with patch("app.agent.nodes.generator.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "SuggestionSet", "args": mock_sugg_set.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await generator_node(state)

            # Assert constraint was passed in compact participant context
            call_args = mock_llm.bind_tools.return_value.ainvoke.call_args[0][0]
            human_payload = json.loads(call_args[1][1])
            assert "constraints" in human_payload["participant_context"]
            assert "Do not commit" in human_payload["participant_context"]["constraints"]

            # Assert suggestion adheres to boundary
            sugg_text = result["suggestions"][0]["text"]
            assert "yes, we will finish by tomorrow" not in sugg_text.lower()
            assert "benchmark" in sugg_text

    def test_05_irrelevant_profile_information_is_excluded(self):
        """5. Compact selector excludes irrelevant expertise keywords, personal name, and unrelated responsibilities."""
        full_context = {
            "preferred_name": "Sandeep",
            "role": "Backend Developer",
            "responsibilities": "Lead social media campaigns and recruitment drives",
            "expertise_keywords": ["Kafka", "PostgreSQL", "React", "CSS", "SEO", "Copywriting"],
            "session_goals": "Advise on Kafka partition sizing",
            "constraints": "Keep suggestions concise",
        }

        compact = select_compact_participant_context(
            participant_context=full_context,
            current_topic="Kafka partition rebalancing and consumer lag",
            conversation_summary="Kafka consumer group lag on audio event topics.",
            conversation_text="We have consumer lag on the Kafka cluster partition rebalancing.",
            response_opportunity="Recommend Kafka partition sizing.",
        )

        assert compact is not None

        # 1. Preferred name is NEVER included for generator
        assert "preferred_name" not in compact

        # 2. Irrelevant keywords (React, CSS, SEO, Copywriting) are excluded
        assert "Kafka" in compact["expertise_keywords"]
        assert "React" not in compact["expertise_keywords"]
        assert "CSS" not in compact["expertise_keywords"]
        assert "SEO" not in compact["expertise_keywords"]
        assert "Copywriting" not in compact["expertise_keywords"]

        # 3. Unrelated responsibilities (social media campaigns) are excluded
        assert "responsibilities" not in compact

        # 4. Relevant role, session goals, and constraints are preserved
        assert compact["role"] == "Backend Developer"
        assert compact["session_goals"] == "Advise on Kafka partition sizing"
        assert compact["constraints"] == "Keep suggestions concise"

    @pytest.mark.anyio
    async def test_06_missing_context_uses_general_generation(self):
        """6. Missing participant context falls back smoothly to general suggestion generation."""
        # 1. Selector returns None
        assert select_compact_participant_context(None) is None
        assert select_compact_participant_context({}) is None

        # 2. Generator node functions without error when participant_context is None
        state: AgentState = {
            "session_id": "test-no-context-06",
            "situation": "General Engineering Sync",
            "objective": "Review system latency",
            "communication_goals": ["Propose latency measurement"],
            "participant_context": None,
            "current_topic": "Pipeline latency",
            "conversation_summary": "Discussion on end-to-end latency benchmarks.",
            "detected_intent": "question",
            "messages": [
                {"sequence_number": 1, "text": "What is our current baseline latency from microphone to suggestion?"}
            ],
            "previous_suggestions": [],
        }

        mock_sugg_set = SuggestionSet(
            suggestions=[
                SuggestionItem(
                    response_type="direct_answer",
                    text="We should profile end-to-end latency with instrumentation timestamps on each service boundary.",
                    rationale="General suggestion grounded in session objective without participant profile.",
                )
            ]
        )

        with patch("app.agent.nodes.generator.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "SuggestionSet", "args": mock_sugg_set.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await generator_node(state)

            # Assert generator LLM was called without participant_context
            call_args = mock_llm.bind_tools.return_value.ainvoke.call_args[0][0]
            human_payload = json.loads(call_args[1][1])
            assert "participant_context" not in human_payload

            # Assert general suggestion generated cleanly
            assert len(result["suggestions"]) == 1
            assert "instrumentation timestamps" in result["suggestions"][0]["text"]
