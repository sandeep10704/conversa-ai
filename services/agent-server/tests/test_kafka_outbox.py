"""Comprehensive test suite for Kafka Event Streaming and Transactional Outbox Pattern.

Tests:
1. Creating a conversation message also creates an outbox record in the same transaction.
2. Database rollback prevents publication of an uncommitted event.
3. Outbox publisher publishes an event successfully and marks it published.
4. Failed publishing leaves the event pending with error information.
5. Retrying publishes pending events after temporary failure.
6. Duplicate event consumption does not process an event twice (idempotency).
7. Events for the same session preserve partition key and chronological order.
8. Invalid events are rejected by the consumer.
9. Failed/invalid events are sent to the Dead Letter Queue (agent.dlq).
10. Outbox processing recovers and publishes pending events after restart.

Does NOT make any OpenRouter or LangGraph calls.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.consumer.worker import TranscriptConsumerWorker
from app.core.kafka_producer import KafkaPublishError
from app.database.models import (
    Base,
    ConversationMessageModel,
    OutboxEventModel,
    ProcessedEventModel,
    SessionModel,
)
from app.database.repository import (
    ConversationRepository,
    OutboxRepository,
    ProcessedEventRepository,
    SessionRepository,
)
from app.events.dlq import DLQEvent
from app.events.topics import KafkaTopics
from app.events.transcript import (
    TranscriptFinalEvent,
    TranscriptFinalPayload,
)
from app.publisher.service import OutboxPublisher
from app.schemas.conversation import TranscriptMessageRequest
from app.schemas.session import CreateSessionRequest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class MockKafkaRecord:
    """Mock consumer record simulating aiokafka ConsumerRecord."""

    def __init__(
        self,
        value: str | bytes,
        topic: str = KafkaTopics.TRANSCRIPT_FINAL.value,
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
    """Mock delivery acknowledgement from Kafka broker."""

    def __init__(self, topic: str, partition: int = 0, offset: int = 1) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class MockKafkaProducer:
    """In-memory mock Kafka producer capturing published events."""

    def __init__(self, should_fail: bool = False, fail_message: str = "Broker timeout") -> None:
        self.published_events: list[dict[str, Any]] = []
        self.should_fail = should_fail
        self.fail_message = fail_message
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
                message=f"Failed to publish event to {topic}: {self.fail_message}",
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


@pytest.fixture
async def test_db_setup():
    """Create isolated SQLite database with full schema including outbox and processed tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_kafka_outbox.db"
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


class TestKafkaOutboxPattern:
    """Test suite covering the 10 core event streaming and outbox requirements."""

    async def test_01_message_creates_outbox_record(self, test_db_setup):
        """1. Verify that creating a conversation message also creates an outbox record in the same tx."""
        session_factory, _, _ = test_db_setup

        async with session_factory() as session:
            async with session.begin():
                session_repo = SessionRepository(session)
                conv_repo = ConversationRepository(session)

                # Create session
                sess = await session_repo.create(
                    CreateSessionRequest(situation="Outbox test", objective="Verify outbox event creation")
                )
                session_id = sess.session_id

                # Add conversation message
                msg_req = TranscriptMessageRequest(
                    text="Testing transactional outbox event creation",
                    source="manual",
                )
                message = await conv_repo.add_message(session_id, msg_req)

        # Query outbox in new transaction to verify persistence
        async with session_factory() as session:
            outbox_stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == message.event_id)
            res = await session.execute(outbox_stmt)
            outbox_row = res.scalar_one_or_none()

            assert outbox_row is not None
            assert outbox_row.aggregate_id == session_id
            assert outbox_row.event_type == KafkaTopics.TRANSCRIPT_FINAL.value
            assert outbox_row.event_version == "v1"
            assert outbox_row.status == "pending"
            assert outbox_row.publish_attempts == 0
            assert outbox_row.published_at is None
            assert outbox_row.last_error is None

            # Verify payload schema
            payload = outbox_row.payload
            assert payload["session_id"] == str(session_id)
            assert payload["sequence_number"] == 1
            assert payload["payload"]["text"] == "Testing transactional outbox event creation"
            assert payload["payload"]["source"] == "manual"

    async def test_02_database_rollback_prevents_uncommitted_event(self, test_db_setup):
        """2. Verify that database rollback prevents publication of an uncommitted event."""
        session_factory, _, _ = test_db_setup

        # First create session
        async with session_factory() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Rollback test", objective="Verify atomicity")
                )
                session_id = sess.session_id

        # Now simulate a transaction failure during message addition
        aborted_event_id = uuid4()
        try:
            async with session_factory() as session:
                async with session.begin():
                    conv_repo = ConversationRepository(session)
                    await conv_repo.add_message(
                        session_id,
                        TranscriptMessageRequest(event_id=aborted_event_id, text="Will be rolled back"),
                    )
                    # Simulate unexpected error before commit
                    raise RuntimeError("Simulated transaction crash!")
        except RuntimeError:
            pass

        # Verify neither message nor outbox event exists in database
        async with session_factory() as session:
            msg_stmt = select(ConversationMessageModel).where(ConversationMessageModel.message_id == aborted_event_id)
            msg_res = await session.execute(msg_stmt)
            assert msg_res.scalar_one_or_none() is None

            outbox_stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == aborted_event_id)
            outbox_res = await session.execute(outbox_stmt)
            assert outbox_res.scalar_one_or_none() is None

    async def test_03_outbox_publisher_publishes_event_successfully(self, test_db_setup):
        """3. Verify that outbox publisher publishes pending event and marks it published."""
        session_factory, _, _ = test_db_setup
        mock_producer = MockKafkaProducer()

        # Step 1: Insert session & message (creating pending outbox event)
        async with session_factory() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Publisher test", objective="Publish to Kafka")
                )
                message = await ConversationRepository(session).add_message(
                    sess.session_id,
                    TranscriptMessageRequest(text="Hello Kafka from Outbox"),
                )

        # Step 2: Run outbox publisher batch
        publisher = OutboxPublisher(
            session_factory=session_factory,
            producer=mock_producer,  # type: ignore[arg-type]
        )
        published_count = await publisher.publish_pending_batch()
        assert published_count == 1

        # Step 3: Verify event was received by Kafka producer with correct key and topic
        assert len(mock_producer.published_events) == 1
        published = mock_producer.published_events[0]
        assert published["topic"] == KafkaTopics.TRANSCRIPT_FINAL.value
        assert published["key"] == str(sess.session_id)
        assert published["event_id"] == str(message.event_id)

        # Step 4: Verify outbox record in DB is now 'published'
        async with session_factory() as session:
            outbox_stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == message.event_id)
            outbox_row = (await session.execute(outbox_stmt)).scalar_one()

            assert outbox_row.status == "published"
            assert outbox_row.published_at is not None
            assert outbox_row.publish_attempts == 1
            assert outbox_row.last_error is None

    async def test_04_failed_publishing_leaves_event_pending(self, test_db_setup):
        """4. Verify that failed publishing leaves event in pending status with error info."""
        session_factory, _, _ = test_db_setup
        failing_producer = MockKafkaProducer(should_fail=True, fail_message="Broker unreachable")

        async with session_factory() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Failure test", objective="Handle broker down")
                )
                message = await ConversationRepository(session).add_message(
                    sess.session_id,
                    TranscriptMessageRequest(text="Message with broker down"),
                )

        publisher = OutboxPublisher(
            session_factory=session_factory,
            producer=failing_producer,  # type: ignore[arg-type]
            max_retries=5,
        )
        await publisher.publish_pending_batch()

        # Verify outbox event remains pending with failure recorded
        async with session_factory() as session:
            outbox_stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == message.event_id)
            outbox_row = (await session.execute(outbox_stmt)).scalar_one()

            assert outbox_row.status == "pending"
            assert outbox_row.publish_attempts == 1
            assert "Broker unreachable" in (outbox_row.last_error or "")
            assert outbox_row.published_at is None

    async def test_05_retrying_publishes_pending_events(self, test_db_setup):
        """5. Verify that retrying publishes pending events after temporary failure."""
        session_factory, _, _ = test_db_setup
        mock_producer = MockKafkaProducer(should_fail=True)

        async with session_factory() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Retry test", objective="Verify retry recovery")
                )
                message = await ConversationRepository(session).add_message(
                    sess.session_id,
                    TranscriptMessageRequest(text="Message to retry"),
                )

        publisher = OutboxPublisher(
            session_factory=session_factory,
            producer=mock_producer,  # type: ignore[arg-type]
        )
        # Attempt 1: Fails
        await publisher.publish_pending_batch()
        assert len(mock_producer.published_events) == 0

        # Broker recovers:
        mock_producer.should_fail = False

        # Attempt 2: Succeeds
        count = await publisher.publish_pending_batch()
        assert count == 1
        assert len(mock_producer.published_events) == 1

        async with session_factory() as session:
            outbox_row = (await session.execute(
                select(OutboxEventModel).where(OutboxEventModel.event_id == message.event_id)
            )).scalar_one()

            assert outbox_row.status == "published"
            assert outbox_row.publish_attempts == 2
            assert outbox_row.published_at is not None
            assert outbox_row.last_error is None

    async def test_06_duplicate_event_consumption_idempotency(self, test_db_setup):
        """6. Verify that duplicate event consumption is idempotent and does not re-process."""
        session_factory, _, _ = test_db_setup
        mock_dlq_producer = MockKafkaProducer()

        worker = TranscriptConsumerWorker(
            session_factory=session_factory,
            dlq_producer=mock_dlq_producer,  # type: ignore[arg-type]
        )

        session_id = uuid4()
        event_id = uuid4()

        event = TranscriptFinalEvent(
            event_id=event_id,
            session_id=session_id,
            sequence_number=1,
            payload=TranscriptFinalPayload(
                sequence_number=1,
                text="Idempotency test utterance",
                source="manual",
            ),
        )
        raw_json = event.model_dump_json()
        record = MockKafkaRecord(value=raw_json, offset=10)

        # First consumption: processed successfully
        res1 = await worker.process_record(record)
        assert res1 is True
        assert worker.processed_count == 1
        assert worker.duplicate_count == 0

        # Verify recorded in processed_events table
        async with session_factory() as session:
            repo = ProcessedEventRepository(session)
            assert await repo.is_processed(event_id) is True

        # Second consumption of identical record: detected as duplicate
        res2 = await worker.process_record(record)
        assert res2 is True
        assert worker.processed_count == 1  # Unchanged
        assert worker.duplicate_count == 1  # Incremented
        assert worker.dlq_count == 0

    async def test_07_events_for_same_session_preserve_order(self, test_db_setup):
        """7. Verify that multiple events for the same session preserve partition key and FIFO order."""
        session_factory, _, _ = test_db_setup
        mock_producer = MockKafkaProducer()

        async with session_factory() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Ordering test", objective="Check FIFO")
                )
                session_id = sess.session_id

                conv_repo = ConversationRepository(session)
                await conv_repo.add_message(session_id, TranscriptMessageRequest(text="Seq 1"))
                await conv_repo.add_message(session_id, TranscriptMessageRequest(text="Seq 2"))
                await conv_repo.add_message(session_id, TranscriptMessageRequest(text="Seq 3"))

        publisher = OutboxPublisher(
            session_factory=session_factory,
            producer=mock_producer,  # type: ignore[arg-type]
        )
        await publisher.publish_pending_batch()

        assert len(mock_producer.published_events) == 3
        # All 3 events share the same partition key (session_id)
        assert all(e["key"] == str(session_id) for e in mock_producer.published_events)

        # Verify sequence numbers are strictly 1 -> 2 -> 3
        seqs = [e["value"]["sequence_number"] for e in mock_producer.published_events]
        assert seqs == [1, 2, 3]

    async def test_08_invalid_events_are_rejected(self, test_db_setup):
        """8. Verify that invalid events fail validation and are rejected without crashing worker."""
        session_factory, _, _ = test_db_setup
        mock_dlq_producer = MockKafkaProducer()

        worker = TranscriptConsumerWorker(
            session_factory=session_factory,
            dlq_producer=mock_dlq_producer,  # type: ignore[arg-type]
        )

        # Missing required sequence_number and payload
        invalid_data = {
            "event_id": str(uuid4()),
            "event_type": KafkaTopics.TRANSCRIPT_FINAL.value,
            "session_id": str(uuid4()),
            # Missing sequence_number and payload!
        }
        record = MockKafkaRecord(value=json.dumps(invalid_data), offset=50)

        # Should handle gracefully and route to DLQ
        handled = await worker.process_record(record)
        assert handled is True
        assert worker.processed_count == 0
        assert worker.dlq_count == 1

    async def test_09_failed_events_sent_to_dlq(self, test_db_setup):
        """9. Verify that completely unparseable messages are routed to agent.dlq."""
        session_factory, _, _ = test_db_setup
        mock_dlq_producer = MockKafkaProducer()

        worker = TranscriptConsumerWorker(
            session_factory=session_factory,
            dlq_producer=mock_dlq_producer,  # type: ignore[arg-type]
        )

        corrupt_record = MockKafkaRecord(value="NOT_VALID_JSON{{{", offset=99)
        handled = await worker.process_record(corrupt_record)
        assert handled is True
        assert worker.dlq_count == 1

        assert len(mock_dlq_producer.published_events) == 1
        dlq_entry = mock_dlq_producer.published_events[0]
        assert dlq_entry["topic"] == KafkaTopics.AGENT_DLQ.value
        dlq_val = dlq_entry["value"]
        assert dlq_val["payload"]["error_stage"] == "json_deserialization"
        assert dlq_val["payload"]["original_topic"] == KafkaTopics.TRANSCRIPT_FINAL.value

    async def test_10_outbox_processing_recovers_after_restart(self, test_db_setup):
        """10. Verify that outbox publisher recovers and processes pending events after process restart."""
        _, _, db_url = test_db_setup

        engine1 = create_async_engine(db_url, echo=False)
        factory1 = async_sessionmaker(engine1, class_=AsyncSession, expire_on_commit=False)

        # Publisher 1 dies before publishing
        async with factory1() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Restart test", objective="Recover outbox")
                )
                msg = await ConversationRepository(session).add_message(
                    sess.session_id,
                    TranscriptMessageRequest(text="Message created before publisher crash"),
                )
        await engine1.dispose()

        # Publisher 2 starts up fresh (simulating container/process restart)
        engine2 = create_async_engine(db_url, echo=False)
        factory2 = async_sessionmaker(engine2, class_=AsyncSession, expire_on_commit=False)
        mock_producer = MockKafkaProducer()

        restarted_publisher = OutboxPublisher(
            session_factory=factory2,
            producer=mock_producer,  # type: ignore[arg-type]
        )
        count = await restarted_publisher.publish_pending_batch()
        assert count == 1
        assert len(mock_producer.published_events) == 1
        assert mock_producer.published_events[0]["event_id"] == str(msg.event_id)

        # Verify DB status is now published
        async with factory2() as session:
            row = (await session.execute(
                select(OutboxEventModel).where(OutboxEventModel.event_id == msg.event_id)
            )).scalar_one()
            assert row.status == "published"
            assert row.published_at is not None

        await engine2.dispose()
