"""Comprehensive Automated Test Suite for Real-Time Agent Orchestration and Suggestion Delivery.

Tests cover 17 distinct scenarios across:
- Agent Trigger Controller (auto-run toggle, thresholding, debouncing, cooldown, concurrency, idempotency, DLQ)
- Independent Agent Worker (command processing, execution tracking, staleness filtering, mock LLM, atomicity, failure handling)
- Delivery Service Integration (suggestion delivery, strict session isolation)

All tests run in MOCK_LLM mode and NEVER make external OpenRouter API calls.
"""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
import importlib.util

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent


def _load_delivery_module(name: str, rel_path: str):
    path = str(BASE_DIR / rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_mod_del_ws = _load_delivery_module("ds_orchestration.websocket", "services/delivery-service/app/websocket.py")
_mod_del_cons = _load_delivery_module("ds_orchestration.consumer", "services/delivery-service/app/consumer.py")
DeliveryConnectionManager = _mod_del_ws.DeliveryConnectionManager
KafkaDeliveryConsumer = _mod_del_cons.KafkaDeliveryConsumer

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.kafka_producer import KafkaPublishError
from app.database.models import (
    AgentExecutionModel,
    Base,
    ConversationMessageModel,
    OutboxEventModel,
    ProcessedEventModel,
    SessionModel,
    SuggestionModel,
)
from app.database.repository import (
    AgentExecutionRepository,
    ConversationRepository,
    OutboxRepository,
    ProcessedEventRepository,
    SessionRepository,
    SuggestionRepository,
)
from app.events.commands import AgentCommandEvent, AgentCommandPayload
from app.events.dlq import DLQEvent
from app.events.topics import KafkaTopics
from app.events.transcript import TranscriptFinalEvent, TranscriptFinalPayload
from app.orchestration.trigger_controller import (
    AgentTriggerController,
    SessionTriggerState,
)
from app.schemas.conversation import TranscriptMessageRequest
from app.schemas.session import CreateSessionRequest
from app.worker.worker import AgentWorker

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class MockKafkaRecord:
    """Mock consumer record simulating aiokafka ConsumerRecord."""

    def __init__(
        self,
        value: str | bytes,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = None,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value if isinstance(value, str) else value.decode("utf-8")


class MockRecordMetadata:
    """Mock broker acknowledgement."""

    def __init__(self, topic: str, partition: int = 0, offset: int = 1) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class MockKafkaProducer:
    """In-memory mock Kafka producer recording published messages."""

    def __init__(self, should_fail: bool = False) -> None:
        self.published_events: List[Dict[str, Any]] = []
        self.should_fail = should_fail
        self._is_started = False
        self._current_offset = 0

    async def start(self) -> None:
        self._is_started = True

    async def stop(self) -> None:
        self._is_started = False

    async def publish(
        self,
        topic: str,
        key: str | UUID | None,
        value: Any,
        event_id: str | UUID | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> MockRecordMetadata:
        if self.should_fail:
            raise KafkaPublishError(
                message=f"Failed to publish event to {topic}",
                event_id=str(event_id) if event_id else None,
                topic=topic,
            )
        self._current_offset += 1
        entry = {
            "topic": topic,
            "key": str(key) if key else None,
            "value": value,
            "event_id": str(event_id) if event_id else None,
            "offset": self._current_offset,
        }
        self.published_events.append(entry)
        return MockRecordMetadata(topic=topic, partition=0, offset=self._current_offset)


class MockWebSocket:
    """Mock WebSocket for delivery service testing."""

    def __init__(self) -> None:
        self.sent_messages: List[str] = []
        self.is_closed = False

    async def send_text(self, text: str) -> None:
        self.sent_messages.append(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.is_closed = True


@pytest.fixture
async def test_db_setup():
    """Create isolated SQLite database with full schema."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_orchestration.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"

        engine = create_async_engine(db_url, echo=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        yield session_factory, engine, db_url

        await engine.dispose()


async def create_test_session_and_messages(
    session_factory: async_sessionmaker[AsyncSession],
    message_count: int = 3,
) -> tuple[UUID, list[int]]:
    """Helper creating a persisted session with n initial messages."""
    session_id: UUID
    seq_numbers: list[int] = []

    async with session_factory() as session:
        async with session.begin():
            s_repo = SessionRepository(session)
            c_repo = ConversationRepository(session)

            req = CreateSessionRequest(
                situation="Architecture review of distributed suggestions system",
                objective="Deliver low latency context-aware recommendations",
                communication_goals=["Ensure reliability", "Avoid high costs"],
                response_preferences={"tone": "professional", "detail_level": "concise"},
                constraints=["Keep suggestion concise"],
            )
            created = await s_repo.create(req)
            session_id = created.session_id

            for i in range(1, message_count + 1):
                msg = await c_repo.add_message(
                    session_id=session_id,
                    request=TranscriptMessageRequest(
                        text=f"Speaker utterance {i} discussing requirements",
                        source="transcription",
                    ),
                )
                seq_numbers.append(msg.sequence_number)

    return session_id, seq_numbers


def create_transcript_record(
    session_id: UUID,
    sequence_number: int,
    text: str = "Test transcript utterance",
    event_id: Optional[UUID] = None,
) -> MockKafkaRecord:
    """Helper building a realistic Kafka record for conversation.transcript.final."""
    eid = event_id or uuid4()
    payload = {
        "event_id": str(eid),
        "event_type": KafkaTopics.TRANSCRIPT_FINAL.value,
        "event_version": "v1",
        "session_id": str(session_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "sequence_number": sequence_number,
        "payload": {
            "sequence_number": sequence_number,
            "text": text,
            "source": "transcription",
        },
    }
    return MockKafkaRecord(
        value=json.dumps(payload),
        topic=KafkaTopics.TRANSCRIPT_FINAL.value,
        key=str(session_id).encode("utf-8"),
    )


class TestAgentOrchestrationSuite:
    """Complete 17-test verification suite for Agent Orchestration and Delivery."""

    # 1. Trigger disabled when AGENT_AUTO_RUN=False
    async def test_01_trigger_disabled_when_auto_run_false(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()
        dlq = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            dlq_producer=dlq,
            auto_run=False,
            min_new_messages=1,
            debounce_ms=0,
        )

        session_id = uuid4()
        record = create_transcript_record(session_id, 1)
        res = await controller.process_record(record)

        assert res is True
        assert len(producer.published_events) == 0
        assert controller.triggers_emitted == 0

        # Verify event marked processed in DB for trigger_controller
        async with session_factory() as s:
            repo = ProcessedEventRepository(s)
            parsed_id = UUID(json.loads(record.value)["event_id"])
            assert await repo.is_processed(parsed_id, consumer_id="trigger_controller")

    # 2. Minimum new messages threshold
    async def test_02_trigger_min_new_messages_threshold(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()
        dlq = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            dlq_producer=dlq,
            auto_run=True,
            min_new_messages=3,
            debounce_ms=10,
            cooldown_seconds=0,
        )

        session_id = uuid4()
        # Send 1st and 2nd messages
        await controller.process_record(create_transcript_record(session_id, 1))
        await controller.process_record(create_transcript_record(session_id, 2))
        await asyncio.sleep(0.05)

        assert len(producer.published_events) == 0
        assert controller.triggers_emitted == 0

        # Send 3rd message - reaches threshold
        await controller.process_record(create_transcript_record(session_id, 3))
        await asyncio.sleep(0.05)

        assert len(producer.published_events) == 1
        assert controller.triggers_emitted == 1
        assert producer.published_events[0]["topic"] == KafkaTopics.AGENT_COMMANDS.value
        assert producer.published_events[0]["value"]["payload"]["processed_sequence_number"] == 3

    # 3. Debounce delays execution
    async def test_03_trigger_debounce_delays_execution(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            debounce_ms=100,  # 100ms debounce
            cooldown_seconds=0,
        )

        session_id = uuid4()
        await controller.process_record(create_transcript_record(session_id, 1))

        # Immediately after message, no trigger fired yet
        assert len(producer.published_events) == 0

        # Wait partial time (30ms) -> still not fired
        await asyncio.sleep(0.03)
        assert len(producer.published_events) == 0

        # Wait remaining time past 100ms
        await asyncio.sleep(0.12)
        assert len(producer.published_events) == 1
        assert controller.triggers_emitted == 1

    # 4. Debounce resets when new speech arrives
    async def test_04_trigger_debounce_reset_on_new_speech(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            debounce_ms=100,
            cooldown_seconds=0,
        )

        session_id = uuid4()
        await controller.process_record(create_transcript_record(session_id, 1))

        # After 50ms, a new utterance arrives (speaker continuing to speak)
        await asyncio.sleep(0.05)
        await controller.process_record(create_transcript_record(session_id, 2))

        # After another 60ms (total 110ms from 1st message), should still NOT have fired
        # because the 2nd message reset the 100ms timer!
        await asyncio.sleep(0.06)
        assert len(producer.published_events) == 0

        # Wait another 60ms (total 120ms from 2nd message) -> now fired!
        await asyncio.sleep(0.06)
        assert len(producer.published_events) == 1
        assert producer.published_events[0]["value"]["payload"]["processed_sequence_number"] == 2

    # 5. Cooldown enforced between consecutive triggers
    async def test_05_trigger_cooldown_enforced(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            debounce_ms=0,
            cooldown_seconds=10.0,  # 10 second cooldown
        )

        session_id = uuid4()
        await controller.process_record(create_transcript_record(session_id, 1))
        assert len(producer.published_events) == 1

        # Second message arrives immediately -> cooldown rejects it
        await controller.process_record(create_transcript_record(session_id, 2))
        assert len(producer.published_events) == 1
        assert controller.triggers_skipped_cooldown == 1

    # 6. Concurrency limit suppresses overlapping executions
    async def test_06_trigger_concurrency_limit(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            debounce_ms=0,
            cooldown_seconds=0,
            max_concurrent_runs=1,
        )

        # Create active running execution in DB for this session
        session_id, _ = await create_test_session_and_messages(session_factory, 1)
        async with session_factory() as session:
            async with session.begin():
                exec_repo = AgentExecutionRepository(session)
                await exec_repo.create_execution(
                    session_id=session_id,
                    trigger_reason="manual_test",
                    processed_sequence_number=1,
                )

        # Arriving transcript tries to trigger
        await controller.process_record(create_transcript_record(session_id, 2))
        assert len(producer.published_events) == 0
        assert controller.triggers_skipped_concurrency == 1

    # 7. Trigger controller idempotency
    async def test_07_trigger_controller_idempotency(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        producer = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            debounce_ms=0,
            cooldown_seconds=0,
        )

        session_id = uuid4()
        event_id = uuid4()
        record = create_transcript_record(session_id, 1, event_id=event_id)

        # First consumption
        await controller.process_record(record)
        assert len(producer.published_events) == 1

        # Redelivered duplicate record
        await controller.process_record(record)
        assert len(producer.published_events) == 1
        assert controller.duplicates_skipped == 1

    # 8. Independent consumer IDs do not collide
    async def test_08_trigger_controller_independent_consumer_id(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id = uuid4()
        event_id = uuid4()

        async with session_factory() as session:
            async with session.begin():
                repo = ProcessedEventRepository(session)
                # Mark as processed by consumer worker
                await repo.mark_processed(
                    event_id=event_id,
                    event_type=KafkaTopics.TRANSCRIPT_FINAL.value,
                    session_id=session_id,
                    consumer_id="transcript_consumer",
                )

        # Trigger controller checks with consumer_id="trigger_controller" -> should NOT be processed
        async with session_factory() as session:
            repo = ProcessedEventRepository(session)
            assert await repo.is_processed(event_id, consumer_id="transcript_consumer") is True
            assert await repo.is_processed(event_id, consumer_id="trigger_controller") is False

    # 9. Trigger controller DLQ on malformed JSON
    async def test_09_trigger_controller_dlq_on_malformed_json(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        dlq = MockKafkaProducer()

        controller = AgentTriggerController(
            session_factory=session_factory,
            dlq_producer=dlq,
        )

        malformed_record = MockKafkaRecord(
            value="{invalid-json-payload-corrupted",
            topic=KafkaTopics.TRANSCRIPT_FINAL.value,
        )
        res = await controller.process_record(malformed_record)

        assert res is True
        assert len(dlq.published_events) == 1
        assert dlq.published_events[0]["topic"] == KafkaTopics.AGENT_DLQ.value
        assert dlq.published_events[0]["value"]["payload"]["error_stage"] == "json_deserialization"

    # 10. Agent Worker consumes command and creates running execution
    async def test_10_agent_worker_consumes_command_and_creates_execution(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id, _ = await create_test_session_and_messages(session_factory, 3)

        worker = AgentWorker(
            session_factory=session_factory,
            mock_llm=True,
        )

        cmd_payload = AgentCommandPayload(
            session_id=session_id,
            trigger_reason="min_messages_test",
            processed_sequence_number=3,
            message_count=3,
        )
        cmd_event = AgentCommandEvent(session_id=session_id, payload=cmd_payload)
        record = MockKafkaRecord(
            value=cmd_event.model_dump_json(),
            topic=KafkaTopics.AGENT_COMMANDS.value,
            key=str(session_id).encode("utf-8"),
        )

        res = await worker.process_record(record)
        assert res is True
        assert worker.executions_started == 1
        assert worker.executions_completed == 1

        # Verify DB execution record
        async with session_factory() as session:
            exec_repo = AgentExecutionRepository(session)
            stmt = select(AgentExecutionModel).where(AgentExecutionModel.session_id == session_id)
            exec_model = (await session.execute(stmt)).scalar_one()
            assert exec_model.execution_status == "completed"
            assert exec_model.processed_sequence_number == 3
            assert exec_model.selected_model == "mock-llm-budget-protection"

    # 11. Stale trigger filtered out by Agent Worker
    async def test_11_agent_worker_stale_trigger_filtered(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id, _ = await create_test_session_and_messages(session_factory, 5)

        # Mark an execution as completed at seq 5
        async with session_factory() as session:
            async with session.begin():
                exec_repo = AgentExecutionRepository(session)
                m = await exec_repo.create_execution(session_id, "earlier_run", 5)
                await exec_repo.update_status(m.execution_id, "completed")

        worker = AgentWorker(
            session_factory=session_factory,
            mock_llm=True,
        )

        # Now an older/stale command arrives with seq 3
        cmd_payload = AgentCommandPayload(
            session_id=session_id,
            trigger_reason="stale_trigger",
            processed_sequence_number=3,
        )
        cmd_event = AgentCommandEvent(session_id=session_id, payload=cmd_payload)
        record = MockKafkaRecord(
            value=cmd_event.model_dump_json(),
            topic=KafkaTopics.AGENT_COMMANDS.value,
        )

        res = await worker.process_record(record)
        assert res is True
        assert worker.executions_stale_skipped == 1
        assert worker.executions_started == 0

    # 12. Mock LLM mode protects OpenRouter budget
    async def test_12_agent_worker_mock_llm_mode(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id, _ = await create_test_session_and_messages(session_factory, 3)

        worker = AgentWorker(
            session_factory=session_factory,
            mock_llm=True,
        )

        cmd_payload = AgentCommandPayload(
            session_id=session_id,
            trigger_reason="test",
            processed_sequence_number=3,
        )
        cmd_event = AgentCommandEvent(session_id=session_id, payload=cmd_payload)
        record = MockKafkaRecord(
            value=cmd_event.model_dump_json(),
            topic=KafkaTopics.AGENT_COMMANDS.value,
        )

        await worker.process_record(record)
        assert worker.suggestions_generated >= 1

        async with session_factory() as session:
            sugg_repo = SuggestionRepository(session)
            items = await sugg_repo.get_suggestions_for_session(session_id)
            assert len(items) >= 1
            assert "latency metrics" in items[0].suggestion_text

    # 13. Atomicity: Suggestion and Outbox Event written together
    async def test_13_agent_worker_atomic_suggestion_and_outbox(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id, _ = await create_test_session_and_messages(session_factory, 3)

        worker = AgentWorker(
            session_factory=session_factory,
            mock_llm=True,
        )

        cmd_payload = AgentCommandPayload(
            session_id=session_id,
            trigger_reason="test",
            processed_sequence_number=3,
        )
        cmd_event = AgentCommandEvent(session_id=session_id, payload=cmd_payload)
        record = MockKafkaRecord(
            value=cmd_event.model_dump_json(),
            topic=KafkaTopics.AGENT_COMMANDS.value,
        )

        await worker.process_record(record)

        async with session_factory() as session:
            # Check suggestion exists
            s_stmt = select(SuggestionModel).where(SuggestionModel.session_id == session_id)
            suggestions = list((await session.execute(s_stmt)).scalars().all())
            assert len(suggestions) == 1

            # Check matching outbox event exists
            o_stmt = select(OutboxEventModel).where(
                OutboxEventModel.aggregate_id == session_id,
                OutboxEventModel.event_type == "agent.suggestions",
            )
            outbox_events = list((await session.execute(o_stmt)).scalars().all())
            assert len(outbox_events) == 1
            assert outbox_events[0].status == "pending"
            assert str(suggestions[0].suggestion_id) == outbox_events[0].payload["payload"]["suggestion_id"]

    # 14. Agent worker idempotency on duplicate commands
    async def test_14_agent_worker_idempotency(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id, _ = await create_test_session_and_messages(session_factory, 3)

        worker = AgentWorker(
            session_factory=session_factory,
            mock_llm=True,
        )

        cmd_payload = AgentCommandPayload(
            session_id=session_id,
            trigger_reason="test",
            processed_sequence_number=3,
        )
        cmd_event = AgentCommandEvent(session_id=session_id, payload=cmd_payload)
        record = MockKafkaRecord(
            value=cmd_event.model_dump_json(),
            topic=KafkaTopics.AGENT_COMMANDS.value,
        )

        # 1st run
        await worker.process_record(record)
        assert worker.executions_started == 1

        # Duplicate redelivery
        await worker.process_record(record)
        assert worker.executions_started == 1
        assert worker.duplicates_skipped == 1

    # 15. Agent worker handles inference failure gracefully
    async def test_15_agent_worker_handles_inference_failure(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        # Create command for nonexistent session to trigger safe failure
        nonexistent_session = uuid4()

        worker = AgentWorker(
            session_factory=session_factory,
            mock_llm=False,  # Triggers lookup failure on nonexistent session
        )

        cmd_payload = AgentCommandPayload(
            session_id=nonexistent_session,
            trigger_reason="test",
            processed_sequence_number=1,
        )
        cmd_event = AgentCommandEvent(session_id=nonexistent_session, payload=cmd_payload)
        record = MockKafkaRecord(
            value=cmd_event.model_dump_json(),
            topic=KafkaTopics.AGENT_COMMANDS.value,
        )

        res = await worker.process_record(record)
        assert res is True

        # Check execution marked failed in DB
        async with session_factory() as session:
            stmt = select(AgentExecutionModel).where(AgentExecutionModel.session_id == nonexistent_session)
            exec_model = (await session.execute(stmt)).scalar_one()
            assert exec_model.execution_status == "failed"
            assert exec_model.failure_stage == "session_not_found"

    # 16. Delivery Service consumes agent.suggestions and broadcasts to session WebSocket
    async def test_16_delivery_service_consumes_suggestions(self):
        manager = DeliveryConnectionManager()
        consumer = KafkaDeliveryConsumer(manager=manager)

        session_id = uuid4()
        ws = MockWebSocket()
        await manager.connect(str(session_id), ws)

        suggestion_payload = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_id),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "session_id": str(session_id),
                "suggestion_id": str(uuid4()),
                "response_type": "perspective",
                "suggestion_text": "Here is an optimized architectural approach.",
                "rationale": "High relevance to topic.",
                "source_sequence_number": 4,
            },
        }

        delivered = await consumer.process_event_payload("agent.suggestions", suggestion_payload)
        assert delivered == 1
        assert len(ws.sent_messages) == 1

        broadcasted = json.loads(ws.sent_messages[0])
        assert broadcasted["type"] == "suggestion"
        assert broadcasted["session_id"] == str(session_id)
        assert broadcasted["text"] == "Here is an optimized architectural approach."
        assert broadcasted["response_type"] == "perspective"

    # 17. Delivery Service strict session isolation
    async def test_17_delivery_service_session_isolation(self):
        manager = DeliveryConnectionManager()
        consumer = KafkaDeliveryConsumer(manager=manager)

        session_a = uuid4()
        session_b = uuid4()

        ws_a = MockWebSocket()
        ws_b = MockWebSocket()

        await manager.connect(str(session_a), ws_a)
        await manager.connect(str(session_b), ws_b)

        # Send suggestion exclusively for Session A
        suggestion_payload = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_a),
            "payload": {
                "session_id": str(session_a),
                "suggestion_id": str(uuid4()),
                "response_type": "perspective",
                "suggestion_text": "Confidential suggestion for session A only",
                "source_sequence_number": 1,
            },
        }

        delivered = await consumer.process_event_payload("agent.suggestions", suggestion_payload)
        assert delivered == 1

        # Session A received message
        assert len(ws_a.sent_messages) == 1
        assert "session A only" in ws_a.sent_messages[0]

        # Session B did NOT receive message (Strict Session Isolation!)
        assert len(ws_b.sent_messages) == 0

    # 18. Duplicate suggestion event processing deduplication in Delivery Service
    async def test_18_delivery_service_deduplicates_repeated_suggestion_events(self):
        manager = DeliveryConnectionManager()
        consumer = KafkaDeliveryConsumer(manager=manager)

        session_id = uuid4()
        sugg_id = uuid4()
        ws = MockWebSocket()
        await manager.connect(str(session_id), ws)

        suggestion_payload = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_id),
            "payload": {
                "session_id": str(session_id),
                "suggestion_id": str(sugg_id),
                "response_type": "perspective",
                "suggestion_text": "Consider reviewing the asynchronous event pipeline latency metrics.",
                "rationale": "High relevance to performance topic.",
                "source_sequence_number": 3,
            },
        }

        # First delivery: should succeed and broadcast to client
        delivered_1 = await consumer.process_event_payload("agent.suggestions", suggestion_payload)
        assert delivered_1 == 1
        assert len(ws.sent_messages) == 1

        # Second delivery (duplicate Kafka offset/event delivery): should be deduplicated and skipped
        delivered_2 = await consumer.process_event_payload("agent.suggestions", suggestion_payload)
        assert delivered_2 == 0
        assert len(ws.sent_messages) == 1  # No duplicate message sent to client!

        # Third delivery: still skipped
        delivered_3 = await consumer.process_event_payload("agent.suggestions", suggestion_payload)
        assert delivered_3 == 0
        assert len(ws.sent_messages) == 1

    # 19. Different legitimate executions can generate separate suggestions even if text is identical
    async def test_19_distinct_executions_with_identical_text_both_delivered(self, test_db_setup):
        session_factory, _, _ = test_db_setup
        session_id, _ = await create_test_session_and_messages(session_factory, 10)

        # Part A: Agent Worker execution for milestone 1 (seq=3) and milestone 2 (seq=7)
        worker = AgentWorker(session_factory=session_factory, mock_llm=True)

        cmd1 = AgentCommandEvent(
            session_id=session_id,
            payload=AgentCommandPayload(
                session_id=session_id,
                trigger_reason="min_messages_debounce",
                processed_sequence_number=3,
            ),
        )
        cmd2 = AgentCommandEvent(
            session_id=session_id,
            payload=AgentCommandPayload(
                session_id=session_id,
                trigger_reason="min_messages_debounce",
                processed_sequence_number=7,
            ),
        )

        res1 = await worker.process_record(MockKafkaRecord(cmd1.model_dump_json(), KafkaTopics.AGENT_COMMANDS.value))
        res2 = await worker.process_record(MockKafkaRecord(cmd2.model_dump_json(), KafkaTopics.AGENT_COMMANDS.value))
        assert res1 is True
        assert res2 is True

        # Verify DB has 2 executions and 2 suggestions with identical text
        async with session_factory() as session:
            sugg_repo = SuggestionRepository(session)
            suggestions = await sugg_repo.get_suggestions_for_session(session_id)
            assert len(suggestions) == 2
            assert suggestions[0].suggestion_text == suggestions[1].suggestion_text
            assert suggestions[0].suggestion_id != suggestions[1].suggestion_id
            assert {suggestions[0].source_last_sequence_number, suggestions[1].source_last_sequence_number} == {3, 7}

        # Part B: Delivery Service delivery of both suggestions
        manager = DeliveryConnectionManager()
        consumer = KafkaDeliveryConsumer(manager=manager)
        ws = MockWebSocket()
        await manager.connect(str(session_id), ws)

        payload_1 = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_id),
            "payload": {
                "session_id": str(session_id),
                "suggestion_id": str(suggestions[0].suggestion_id),
                "response_type": "perspective",
                "suggestion_text": suggestions[0].suggestion_text,
                "source_sequence_number": suggestions[0].source_last_sequence_number,
            },
        }
        payload_2 = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_id),
            "payload": {
                "session_id": str(session_id),
                "suggestion_id": str(suggestions[1].suggestion_id),
                "response_type": "perspective",
                "suggestion_text": suggestions[1].suggestion_text,
                "source_sequence_number": suggestions[1].source_last_sequence_number,
            },
        }

        # Both events must be delivered without suppression!
        deliv1 = await consumer.process_event_payload("agent.suggestions", payload_1)
        deliv2 = await consumer.process_event_payload("agent.suggestions", payload_2)
        assert deliv1 == 1
        assert deliv2 == 1
        assert len(ws.sent_messages) == 2

        msg1 = json.loads(ws.sent_messages[0])
        msg2 = json.loads(ws.sent_messages[1])
        assert msg1["suggestion_id"] != msg2["suggestion_id"]
        assert msg1["text"] == msg2["text"]
        assert msg1["source_sequence_number"] != msg2["source_sequence_number"]

    # 20. Multi-session isolation under concurrent suggestions
    async def test_20_multi_session_strict_isolation_concurrent(self):
        manager = DeliveryConnectionManager()
        consumer = KafkaDeliveryConsumer(manager=manager)

        session_a = uuid4()
        session_b = uuid4()
        session_c = uuid4()

        ws_a = MockWebSocket()
        ws_b = MockWebSocket()
        ws_c = MockWebSocket()

        await manager.connect(str(session_a), ws_a)
        await manager.connect(str(session_b), ws_b)
        await manager.connect(str(session_c), ws_c)

        # Dispatch identical suggestions to Session A and Session B
        shared_text = "Consider reviewing the asynchronous event pipeline latency metrics."
        payload_a = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_a),
            "payload": {
                "session_id": str(session_a),
                "suggestion_id": str(uuid4()),
                "response_type": "perspective",
                "suggestion_text": shared_text,
                "source_sequence_number": 3,
            },
        }
        payload_b = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_b),
            "payload": {
                "session_id": str(session_b),
                "suggestion_id": str(uuid4()),
                "response_type": "perspective",
                "suggestion_text": shared_text,
                "source_sequence_number": 3,
            },
        }

        deliv_a = await consumer.process_event_payload("agent.suggestions", payload_a)
        deliv_b = await consumer.process_event_payload("agent.suggestions", payload_b)
        assert deliv_a == 1
        assert deliv_b == 1

        # Session A only gets Session A
        assert len(ws_a.sent_messages) == 1
        assert json.loads(ws_a.sent_messages[0])["session_id"] == str(session_a)

        # Session B only gets Session B
        assert len(ws_b.sent_messages) == 1
        assert json.loads(ws_b.sent_messages[0])["session_id"] == str(session_b)

        # Session C gets nothing (Strict Isolation!)
        assert len(ws_c.sent_messages) == 0

    # 21. Reconnecting a WebSocket does not produce duplicate live delivery
    async def test_21_websocket_reconnection_no_duplicate_live_delivery(self):
        manager = DeliveryConnectionManager()
        consumer = KafkaDeliveryConsumer(manager=manager)

        session_id = uuid4()
        ws_old = MockWebSocket()
        ws_new = MockWebSocket()

        # Step 1: Client connects with first socket
        await manager.connect(str(session_id), ws_old)

        # Step 2: Client disconnects and reconnects with new socket
        await manager.disconnect(str(session_id), ws_old)
        await manager.connect(str(session_id), ws_new)

        # Step 3: Suggestion arrives after reconnection
        suggestion_payload = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_id),
            "payload": {
                "session_id": str(session_id),
                "suggestion_id": str(uuid4()),
                "response_type": "perspective",
                "suggestion_text": "Live suggestion delivered after reconnect",
                "source_sequence_number": 5,
            },
        }

        delivered = await consumer.process_event_payload("agent.suggestions", suggestion_payload)
        assert delivered == 1

        # Disconnected socket did not receive any live message
        assert len(ws_old.sent_messages) == 0

        # Newly reconnected socket received exactly one delivery
        assert len(ws_new.sent_messages) == 1
        assert "after reconnect" in ws_new.sent_messages[0]

        # Step 4: Stale dead socket handling: if a socket breaks, it is evicted without duplicating to healthy socket
        class BrokenWebSocket(MockWebSocket):
            async def send_text(self, text: str) -> None:
                raise ConnectionResetError("Client dropped connection")

        ws_broken = BrokenWebSocket()
        await manager.connect(str(session_id), ws_broken)

        subsequent_payload = {
            "event_type": "agent.suggestions",
            "aggregate_id": str(session_id),
            "payload": {
                "session_id": str(session_id),
                "suggestion_id": str(uuid4()),
                "response_type": "perspective",
                "suggestion_text": "Subsequent suggestion after broken socket eviction",
                "source_sequence_number": 6,
            },
        }

        delivered_sub = await consumer.process_event_payload("agent.suggestions", subsequent_payload)
        # Delivered to the 1 healthy client (ws_new); ws_broken was pruned
        assert delivered_sub == 1
        assert len(ws_new.sent_messages) == 2
