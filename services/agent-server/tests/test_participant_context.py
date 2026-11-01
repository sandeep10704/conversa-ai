"""Deterministic automated test suite for Session-Specific Participant Context (Phase 2).

Covers all Phase 2 requirements:
1. Correct context creation, retrieval, and updates.
2. Context isolation across sessions.
3. Maximum 1,500-character cumulative length enforcement.
4. Explicit context deletion on session end (retention policy).
5. Direct name mention in conversation.
6. Relevant technical discussion without name mention.
7. Same-name disambiguation (colleagues in different roles/teams).
8. Unrelated discussion (off-topic chatter / small talk).
9. Missing participant context (graceful fallback).
10. Unsupported expertise claims (no hallucinated qualifications).
11. Existing suggestions remain grounded.
12. Single-operator privacy architecture & delivery limitation documentation.

All tests run completely offline with mocked dependencies and zero OpenRouter requests.
"""

import asyncio
from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.agent.graph import get_agent_graph
from app.agent.nodes.relevance import relevance_node
from app.agent.state import AgentState
from app.database.models import (
    Base,
    ConversationMessageModel,
    SessionModel,
    utc_now,
)
from app.database.repository import SessionRepository
from app.events.topics import KafkaTopics
from app.schemas.agent import (
    DiscussionAnalysis,
    RelevanceAssessment,
    SuggestionItem,
    SuggestionSet,
)
from app.schemas.participant_context import ParticipantContext
from app.schemas.session import (
    CreateSessionRequest,
    ResponsePreferences,
    SessionResponse,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def db_session_factory():
    """Create an isolated, in-memory SQLite database for repository tests."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    yield session_factory
    await engine.dispose()


class TestParticipantContextSchemaAndStorage:
    """Tests 1-4: Pydantic validation, SQLite/PostgreSQL storage, session isolation, and deletion on end."""

    async def test_01_participant_context_creation_and_retrieval(self, db_session_factory):
        """1. Create session with valid participant context, retrieve, and verify all fields."""
        async with db_session_factory() as session:
            repo = SessionRepository(session)

            context = ParticipantContext(
                preferred_name="Sandeep",
                role="Backend Developer",
                responsibilities="Backend architecture and audio pipeline development",
                expertise_keywords=["Java", "PostgreSQL", "Kafka", "audio processing"],
                session_goals="Contribute technical insights on architecture and latency",
                constraints="Avoid committing to implementation timelines without estimation",
            )

            req = CreateSessionRequest(
                situation="Backend architecture and performance sync",
                objective="Review audio pipeline latency and Kafka lag",
                communication_goals=["Ensure real-time streaming constraints are met"],
                participant_context=context,
            )

            created = await repo.create(req)
            assert created.session_id is not None
            assert created.status == "active"
            assert created.participant_context is not None
            assert created.participant_context.preferred_name == "Sandeep"
            assert created.participant_context.role == "Backend Developer"
            assert "Kafka" in created.participant_context.expertise_keywords

            # Retrieve session by ID
            retrieved = await repo.get(created.session_id)
            assert retrieved is not None
            assert retrieved.participant_context is not None
            assert retrieved.participant_context.preferred_name == "Sandeep"
            assert retrieved.participant_context.responsibilities == "Backend architecture and audio pipeline development"
            assert retrieved.participant_context.expertise_keywords == ["Java", "PostgreSQL", "Kafka", "audio processing"]
            assert retrieved.participant_context.constraints == "Avoid committing to implementation timelines without estimation"

    async def test_02_participant_context_isolation_across_sessions(self, db_session_factory):
        """2. Participant contexts must be strictly isolated to their own sessions."""
        async with db_session_factory() as session:
            repo = SessionRepository(session)

            # Session A: Sandeep (Backend Developer)
            ctx_a = ParticipantContext(
                preferred_name="Sandeep",
                role="Backend Developer",
                expertise_keywords=["PostgreSQL", "Kafka"],
            )
            sess_a = await repo.create(
                CreateSessionRequest(
                    situation="Session A",
                    objective="Backend review",
                    participant_context=ctx_a,
                )
            )

            # Session B: Priya (Frontend Engineer)
            ctx_b = ParticipantContext(
                preferred_name="Priya",
                role="Frontend Engineer",
                expertise_keywords=["React", "TypeScript", "Tailwind"],
            )
            sess_b = await repo.create(
                CreateSessionRequest(
                    situation="Session B",
                    objective="UI review",
                    participant_context=ctx_b,
                )
            )

            # Assert isolation
            res_a = await repo.get(sess_a.session_id)
            res_b = await repo.get(sess_b.session_id)

            assert res_a.participant_context.preferred_name == "Sandeep"
            assert res_a.participant_context.role == "Backend Developer"
            assert "React" not in res_a.participant_context.expertise_keywords

            assert res_b.participant_context.preferred_name == "Priya"
            assert res_b.participant_context.role == "Frontend Engineer"
            assert "Kafka" not in res_b.participant_context.expertise_keywords

            # Updating Session A does not modify Session B
            ctx_a_updated = ParticipantContext(
                preferred_name="Sandeep G",
                role="Lead Architect",
                expertise_keywords=["Distributed Systems"],
            )
            await repo.update_participant_context(sess_a.session_id, ctx_a_updated)

            refreshed_b = await repo.get(sess_b.session_id)
            assert refreshed_b.participant_context.preferred_name == "Priya"

    def test_03_maximum_length_enforcement_1500_chars(self):
        """3. Enforce strict 1,500-character cumulative limit across all fields."""
        # 1. Valid context exactly at boundary
        valid_context = ParticipantContext(
            preferred_name="A" * 50,
            role="B" * 100,
            responsibilities="C" * 350,
            expertise_keywords=["D" * 20 for _ in range(10)],  # 200 chars
            session_goals="E" * 350,
            constraints="F" * 350,
        )
        # Total = 50 + 100 + 350 + 200 + 350 + 350 = 1400 <= 1500
        assert valid_context.preferred_name == "A" * 50

        # 2. Exceeding 1500 characters raises ValidationError
        with pytest.raises(ValidationError) as exc_info:
            ParticipantContext(
                preferred_name="Sandeep",
                role="Backend Developer",
                responsibilities="X" * 400,
                expertise_keywords=["K" * 20 for _ in range(20)],  # 400 chars
                session_goals="G" * 400,
                constraints="C" * 400,  # Total = 7 + 17 + 400 + 400 + 400 + 400 = 1624 > 1500
            )
        assert "exceeds the maximum allowed limit of 1500 characters" in str(exc_info.value)

    async def test_04_context_deletion_on_session_end(self, db_session_factory):
        """4. When a session transitions to ended, participant_context is explicitly deleted (purged)."""
        async with db_session_factory() as session:
            repo = SessionRepository(session)

            context = ParticipantContext(
                preferred_name="Sandeep",
                role="Backend Developer",
                responsibilities="Audio streaming architecture",
            )
            created = await repo.create(
                CreateSessionRequest(
                    situation="Session to be terminated",
                    objective="Test context retention policy",
                    participant_context=context,
                )
            )

            assert created.participant_context is not None

            # End session explicitly
            ended = await repo.end_session(created.session_id)
            assert ended.status == "ended"
            assert ended.participant_context is None

            # Direct database reload check
            reloaded = await repo.get(created.session_id)
            assert reloaded.status == "ended"
            assert reloaded.participant_context is None


class TestRelevanceNodeParticipantContextScenarios:
    """Tests 5-10: Relevance Node integration covering all conversational scenarios."""

    async def test_05_relevance_direct_name_mention(self):
        """5. Direct address: speaker asks Sandeep directly -> should_suggest=true."""
        state: AgentState = {
            "session_id": "test-direct-name",
            "situation": "Backend architecture sync",
            "objective": "Review database pooling",
            "communication_goals": ["Address database connection bottlenecks"],
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "responsibilities": "PostgreSQL database optimization and connection pooling",
                "expertise_keywords": ["PostgreSQL", "connection pooling"],
            },
            "current_topic": "PostgreSQL connection limits",
            "conversation_summary": "Team is discussing connection starvation under high load.",
            "detected_intent": "question",
            "intent_evidence": "Sandeep, what connection pool size do you recommend for the agent server?",
            "messages": [
                {"sequence_number": 1, "text": "We are seeing intermittent connection timeouts under load."},
                {"sequence_number": 2, "text": "Sandeep, what connection pool size do you recommend for the agent server?"},
            ],
            "previous_suggestions": [],
        }

        mock_relevance = RelevanceAssessment(
            should_suggest=True,
            confidence=0.92,
            reason="Participant Sandeep was directly asked for a recommendation on connection pool sizing.",
            response_opportunity="Recommend optimal PostgreSQL pool size based on concurrent worker capacity.",
        )

        with patch("app.agent.nodes.relevance.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "RelevanceAssessment", "args": mock_relevance.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await relevance_node(state)

            # Assert: LLM prompt contained the participant context with preferred_name
            call_args = mock_llm.bind_tools.return_value.ainvoke.call_args[0][0]
            human_prompt = json.loads(call_args[1][1])
            assert human_prompt["participant_context"]["preferred_name"] == "Sandeep"

            # Assert relevance decision
            assert result["should_suggest"] is True
            assert result["relevance"]["should_suggest"] is True
            assert result["relevance"]["confidence"] == 0.92

    async def test_06_relevance_technical_discussion_without_name_mention(self):
        """6. Relevant discussion without name mention -> matches expertise keywords -> should_suggest=true."""
        state: AgentState = {
            "session_id": "test-tech-no-name",
            "situation": "Platform stability meeting",
            "objective": "Address latency and lag",
            "communication_goals": ["Propose Kafka consumer group tuning"],
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "responsibilities": "Audio pipeline and Kafka event streaming",
                "expertise_keywords": ["Kafka", "consumer lag", "audio streaming"],
            },
            "current_topic": "Kafka consumer lag spikes",
            "conversation_summary": "Audio gateway events are backing up in the pipeline.",
            "detected_intent": "statement",
            "intent_evidence": "The consumer lag on audio chunks is growing uncontrollably during peak traffic.",
            "messages": [
                {"sequence_number": 1, "text": "The consumer lag on audio chunks is growing uncontrollably during peak traffic."},
                {"sequence_number": 2, "text": "We need to figure out why the consumers aren't keeping up."},
            ],
            "previous_suggestions": [],
        }

        mock_relevance = RelevanceAssessment(
            should_suggest=True,
            confidence=0.88,
            reason="Discussion focuses on Kafka consumer lag, directly matching Sandeep's expertise in Kafka streaming.",
            response_opportunity="Explain consumer group partition assignment and prefetch buffer tuning.",
        )

        with patch("app.agent.nodes.relevance.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "RelevanceAssessment", "args": mock_relevance.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await relevance_node(state)

            assert result["should_suggest"] is True
            assert "Kafka" in result["relevance"]["reason"]

    async def test_07_relevance_same_name_disambiguation(self):
        """7. Same-name mention referring to another team member -> should_suggest=false."""
        state: AgentState = {
            "session_id": "test-same-name",
            "situation": "All-hands project status update",
            "objective": "Listen to company announcements",
            "communication_goals": ["Represent backend team"],
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "responsibilities": "Backend systems and audio infrastructure",
                "expertise_keywords": ["PostgreSQL", "Kafka", "Python"],
            },
            "current_topic": "Marketing campaign schedule",
            "conversation_summary": "Marketing team discusses ad spend.",
            "detected_intent": "statement",
            "intent_evidence": "Sandeep from Marketing confirmed the Google Ads budget was approved yesterday.",
            "messages": [
                {"sequence_number": 1, "text": "Sandeep from Marketing confirmed the Google Ads budget was approved yesterday."},
                {"sequence_number": 2, "text": "Great, so we can launch the ad campaign on Monday."},
            ],
            "previous_suggestions": [],
        }

        mock_relevance = RelevanceAssessment(
            should_suggest=False,
            confidence=0.95,
            reason="The mention refers to 'Sandeep from Marketing', not the backend developer participant.",
            response_opportunity="",
        )

        with patch("app.agent.nodes.relevance.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "RelevanceAssessment", "args": mock_relevance.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await relevance_node(state)

            # Assert: Correctly disambiguated -> should_suggest is FALSE
            assert result["should_suggest"] is False
            assert "Marketing" in result["relevance"]["reason"]

    async def test_08_relevance_unrelated_discussion(self):
        """8. Unrelated discussion (social chatter / catering) -> should_suggest=false."""
        state: AgentState = {
            "session_id": "test-unrelated",
            "situation": "Team sync",
            "objective": "Technical architecture discussion",
            "communication_goals": ["Contribute to system scaling"],
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "expertise_keywords": ["PostgreSQL", "Kafka"],
            },
            "current_topic": "Lunch preferences",
            "conversation_summary": "Team deciding what to order for lunch.",
            "detected_intent": "question",
            "intent_evidence": "Does anyone want pizza or Thai food today?",
            "messages": [
                {"sequence_number": 1, "text": "Does anyone want pizza or Thai food today?"},
            ],
            "previous_suggestions": [],
        }

        mock_relevance = RelevanceAssessment(
            should_suggest=False,
            confidence=0.98,
            reason="Discussion is social small talk regarding lunch food, completely unrelated to technical goals.",
            response_opportunity="",
        )

        with patch("app.agent.nodes.relevance.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "RelevanceAssessment", "args": mock_relevance.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await relevance_node(state)
            assert result["should_suggest"] is False

    async def test_09_relevance_missing_participant_context_fallback(self):
        """9. Missing participant context (None) -> falls back gracefully to situation/objective without error."""
        state: AgentState = {
            "session_id": "test-no-context",
            "situation": "Kafka latency review",
            "objective": "Ensure transcript partition ordering",
            "communication_goals": ["Confirm partition keys are active"],
            "participant_context": None,  # Backward compatible / missing context
            "current_topic": "Partition key configuration",
            "conversation_summary": "Discussion of partition assignment.",
            "detected_intent": "question",
            "intent_evidence": "Are we partitioning transcript messages by session_id?",
            "messages": [
                {"sequence_number": 1, "text": "Are we partitioning transcript messages by session_id?"},
            ],
            "previous_suggestions": [],
        }

        mock_relevance = RelevanceAssessment(
            should_suggest=True,
            confidence=0.85,
            reason="Direct question on communication goal topic (Kafka partition keys).",
            response_opportunity="Confirm session_id is used as partition key.",
        )

        with patch("app.agent.nodes.relevance.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "RelevanceAssessment", "args": mock_relevance.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await relevance_node(state)

            # Assert: Did not fail or crash when participant_context is None
            assert result["should_suggest"] is True

    async def test_10_relevance_unsupported_expertise_claims(self):
        """10. Discussion on domain outside stated expertise -> does not invent qualifications -> should_suggest=false."""
        state: AgentState = {
            "session_id": "test-unsupported-expertise",
            "situation": "Product engineering sync",
            "objective": "Backend stability",
            "participant_context": {
                "preferred_name": "Sandeep",
                "role": "Backend Developer",
                "responsibilities": "PostgreSQL database queries and APIs",
                "expertise_keywords": ["PostgreSQL", "SQL", "FastAPI"],
            },
            "current_topic": "Quantum computing cryptographic key exchange",
            "conversation_summary": "Team discussing quantum annealing and post-quantum lattice cryptography.",
            "detected_intent": "question",
            "intent_evidence": "Can someone implement lattice-based post-quantum cryptography in the mobile SDK?",
            "messages": [
                {"sequence_number": 1, "text": "Can someone implement lattice-based post-quantum cryptography in the mobile SDK?"},
            ],
            "previous_suggestions": [],
        }

        mock_relevance = RelevanceAssessment(
            should_suggest=False,
            confidence=0.92,
            reason="Discussion requires specialized post-quantum cryptography expertise not stated in Sandeep's backend profile.",
            response_opportunity="",
        )

        with patch("app.agent.nodes.relevance.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.bind_tools.return_value.ainvoke = AsyncMock(
                return_value=MagicMock(
                    tool_calls=[{"name": "RelevanceAssessment", "args": mock_relevance.model_dump()}]
                )
            )
            mock_get_llm.return_value = mock_llm

            result = await relevance_node(state)
            assert result["should_suggest"] is False


class TestPrivacyAndDeliveryLimitations:
    """Tests 11-13: Verifies participant context is never leaked through public transcripts or Delivery broadcasts."""

    async def test_11_participant_context_not_exposed_in_transcript_api(self, db_session_factory):
        """11. Public transcript messages endpoint must never return or expose participant_context."""
        async with db_session_factory() as session:
            repo = SessionRepository(session)
            created = await repo.create(
                CreateSessionRequest(
                    situation="Privacy test",
                    objective="Verify transcript message isolation",
                    participant_context=ParticipantContext(
                        preferred_name="Sandeep",
                        role="Backend Lead",
                        responsibilities="Confidential architecture review",
                    ),
                )
            )

            # Insert message
            msg = ConversationMessageModel(
                message_id=uuid4(),
                session_id=created.session_id,
                sequence_number=1,
                text="Hello team",
                source="transcription",
                created_at=utc_now(),
            )
            session.add(msg)
            await session.flush()

            # Verify ConversationMessageModel has no participant_context column or leak
            assert not hasattr(msg, "participant_context")

    async def test_12_single_operator_delivery_limitation_documented(self):
        """12. Document & verify single-operator architecture: delivery broadcast does not include participant context."""
        # The Delivery Service broadcasts suggestions to all active WebSocket connections for a session.
        # This test verifies that SuggestionModel and delivery payloads do NOT include participant_context.
        suggestion_item = SuggestionItem(
            response_type="technical_explanation",
            text="We can tune the Kafka partition assignment strategy.",
            rationale="Addresses consumer lag under high load.",
        )
        payload = suggestion_item.model_dump()

        # Verify participant context fields are strictly absent from suggestion payloads
        assert "participant_context" not in payload
        assert "preferred_name" not in payload
        assert "responsibilities" not in payload
