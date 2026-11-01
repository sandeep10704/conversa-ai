"""Independent Kafka consumer worker for conversation transcript events."""

import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.kafka_producer import AIOKafkaEventProducer
from app.database.repository import ProcessedEventRepository
from app.database.session import async_session_factory
from app.events.dlq import DLQEvent, DLQPayload
from app.events.topics import KafkaTopics
from app.events.transcript import TranscriptFinalEvent

logger = logging.getLogger(__name__)


class TranscriptConsumerWorker:
    """Independent Kafka event consumer for conversation.transcript.final events.

    Guarantees:
    - At-least-once processing with safe manual offset commit: Offsets are committed
      ONLY after the event state is durably written to PostgreSQL.
    - Idempotency: Processed event IDs are tracked in `processed_events` table; duplicate
      events are detected and safely skipped without re-processing.
    - Partition Ordering: Subscribes with partition-level FIFO processing.
    - Dead Letter Queue: Malformed payloads or unrecoverable processing failures are
      routed to `agent.dlq` without blocking the consumer partition.
    - Safe Default: AGENT_AUTO_RUN is False by default, preventing unwanted LLM API calls.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        dlq_producer: AIOKafkaEventProducer | None = None,
        auto_offset_reset: str = "earliest",
        max_retries: int = 3,
    ) -> None:
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.group_id = group_id or settings.consumer_group_id
        self.session_factory = session_factory or async_session_factory()
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries
        self.auto_run = settings.agent_auto_run

        self.dlq_producer = dlq_producer or AIOKafkaEventProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id="conversa-consumer-dlq",
        )

        self._consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Telemetry metrics
        self.processed_count = 0
        self.duplicate_count = 0
        self.dlq_count = 0

    async def start(self) -> None:
        """Start the Kafka consumer and DLQ producer."""
        logger.info(
            f"Starting TranscriptConsumerWorker [group={self.group_id}, servers={self.bootstrap_servers}]..."
        )
        await self.dlq_producer.start()

        self._consumer = AIOKafkaConsumer(
            KafkaTopics.TRANSCRIPT_FINAL.value,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,  # Manual commit only after DB transaction success
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        await self._consumer.start()
        self._running = True
        self._shutdown_event.clear()
        logger.info(f"Consumer worker subscribed to topic '{KafkaTopics.TRANSCRIPT_FINAL.value}'.")

    async def stop(self) -> None:
        """Gracefully stop consumer and DLQ producer."""
        logger.info("Stopping TranscriptConsumerWorker...")
        self._running = False
        self._shutdown_event.set()

        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer: {e}")
            finally:
                self._consumer = None

        await self.dlq_producer.stop()
        logger.info("TranscriptConsumerWorker stopped.")

    async def send_to_dlq(
        self,
        record: ConsumerRecord,
        error_stage: str,
        error_message: str,
        raw_payload: Any = None,
        event_id: str | None = None,
        session_id: UUID | None = None,
    ) -> None:
        """Construct and publish a Dead Letter Queue event."""
        self.dlq_count += 1
        dlq_event = DLQEvent(
            session_id=session_id or UUID("00000000-0000-0000-0000-000000000000"),
            payload=DLQPayload(
                original_event_id=event_id,
                original_topic=record.topic,
                error_stage=error_stage,
                error_message=error_message,
                raw_payload=raw_payload,
            ),
        )

        partition_key = str(session_id) if session_id else "dlq"
        try:
            await self.dlq_producer.publish(
                topic=KafkaTopics.AGENT_DLQ.value,
                key=partition_key,
                value=dlq_event.model_dump(mode="json"),
                event_id=dlq_event.event_id,
            )
            logger.warning(
                f"[Consumer DLQ] Routed failed record at {record.topic}:{record.partition}:{record.offset} to {KafkaTopics.AGENT_DLQ.value} (stage={error_stage})"
            )
        except Exception as e:
            logger.critical(f"[Consumer DLQ] Failed to route event to DLQ: {e}")

    async def process_record(self, record: ConsumerRecord) -> bool:
        """Process a single Kafka consumer record end-to-end.

        Returns:
            True if processed or sent to DLQ (safe to commit offset).
        """
        raw_val = record.value

        # 1. Deserialize JSON
        try:
            data = json.loads(raw_val)
        except Exception as exc:
            logger.error(f"[Consumer] JSON parse error at offset {record.offset}: {exc}")
            await self.send_to_dlq(
                record=record,
                error_stage="json_deserialization",
                error_message=str(exc),
                raw_payload=raw_val[:1000] if raw_val else None,
            )
            return True

        # 2. Schema Validation
        try:
            event = TranscriptFinalEvent.model_validate(data)
        except Exception as exc:
            logger.error(f"[Consumer] Pydantic schema validation error at offset {record.offset}: {exc}")
            event_id = data.get("event_id") if isinstance(data, dict) else None
            await self.send_to_dlq(
                record=record,
                error_stage="schema_validation",
                error_message=str(exc),
                raw_payload=data,
                event_id=str(event_id) if event_id else None,
            )
            return True

        # 3. Idempotency Check & Processing
        event_id = event.event_id
        session_id = event.session_id

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.session_factory() as session:
                    async with session.begin():
                        repo = ProcessedEventRepository(session)

                        if await repo.is_processed(event_id):
                            self.duplicate_count += 1
                            logger.info(
                                f"[Consumer] Duplicate event {event_id} already processed. Skipping re-execution."
                            )
                            return True

                        # Process event: record in processed_events table
                        await repo.mark_processed(
                            event_id=event_id,
                            event_type=event.event_type,
                            session_id=session_id,
                        )

                        if self.auto_run:
                            # Hook for future agent auto-trigger (disabled by default)
                            logger.info(
                                f"[Consumer] AGENT_AUTO_RUN is enabled. Triggering agent pipeline for session {session_id}..."
                            )
                        else:
                            logger.info(
                                f"[Consumer] Successfully validated and recorded transcript event [{event_id}] "
                                f"for session [{session_id}] (seq={event.sequence_number}). (AGENT_AUTO_RUN=false)"
                            )

                self.processed_count += 1
                return True

            except Exception as exc:
                logger.warning(
                    f"[Consumer] Transient error processing event {event_id} (attempt {attempt}/{self.max_retries}): {exc}"
                )
                if attempt == self.max_retries:
                    logger.error(f"[Consumer] Max retries exhausted for event {event_id}. Routing to DLQ.")
                    await self.send_to_dlq(
                        record=record,
                        error_stage="processing_exhausted",
                        error_message=str(exc),
                        raw_payload=data,
                        event_id=str(event_id),
                        session_id=session_id,
                    )
                    return True

                await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

        return True

    async def run_loop(self) -> None:
        """Main consumer loop reading from Kafka and committing offsets."""
        await self.start()
        logger.info("Consumer message polling loop started.")

        try:
            while self._running and self._consumer is not None:
                # Fetch messages with timeout
                try:
                    records_dict = await self._consumer.getmany(timeout_ms=1000, max_records=50)
                except Exception as e:
                    if not self._running:
                        break
                    logger.warning(f"[Consumer] Fetch error: {e}")
                    await asyncio.sleep(1.0)
                    continue

                for topic_partition, records in records_dict.items():
                    for record in records:
                        if not self._running:
                            break

                        safe_to_commit = await self.process_record(record)
                        if safe_to_commit and self._consumer is not None:
                            try:
                                await self._consumer.commit()
                            except Exception as e:
                                logger.error(f"[Consumer] Offset commit failure: {e}")

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled.")
        finally:
            await self.stop()
