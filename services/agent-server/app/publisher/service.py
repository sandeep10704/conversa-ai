"""Transactional Outbox Publisher service for Conversa AI."""

import asyncio
import logging
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.kafka_producer import AIOKafkaEventProducer, KafkaPublishError
from app.database.repository import OutboxRepository
from app.database.session import async_session_factory

logger = logging.getLogger(__name__)


class OutboxPublisher:
    """Independent worker that polls PostgreSQL outbox_events and publishes to Kafka.

    Guarantees:
    - At-least-once delivery: Outbox records are marked 'published' ONLY after broker ACK.
    - Concurrency-safe: Uses SELECT FOR UPDATE SKIP LOCKED to prevent duplicate claims.
    - Chronological ordering: Events are fetched FIFO (ORDER BY created_at ASC) and
      partitioned by session_id so partition order mirrors database commit order.
    - Fault-tolerant: Exponential backoff on transient errors; process restarts resume pending events.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        producer: AIOKafkaEventProducer | None = None,
        poll_interval_seconds: float | None = None,
        batch_size: int | None = None,
        max_retries: int | None = None,
    ) -> None:
        settings = get_settings()
        self.session_factory = session_factory or async_session_factory()
        self.poll_interval = poll_interval_seconds or settings.outbox_poll_interval_seconds
        self.batch_size = batch_size or settings.outbox_batch_size
        self.max_retries = max_retries or settings.outbox_max_retries

        self.producer = producer or AIOKafkaEventProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="conversa-outbox-publisher",
        )

        self._running = False
        self._shutdown_event = asyncio.Event()

        # Telemetry metrics
        self.published_count = 0
        self.failed_count = 0
        self.poll_cycles = 0

    async def start(self) -> None:
        """Start the publisher and connect the Kafka producer."""
        logger.info("Starting Transactional Outbox Publisher...")
        await self.producer.start()
        self._running = True
        self._shutdown_event.clear()

    async def stop(self) -> None:
        """Signal the publisher loop to stop and flush connections."""
        logger.info("Stopping Transactional Outbox Publisher...")
        self._running = False
        self._shutdown_event.set()
        await self.producer.stop()
        logger.info("Transactional Outbox Publisher stopped.")

    async def publish_pending_batch(self) -> int:
        """Fetch and publish one batch of pending outbox events.

        Returns:
            Number of events claimed in this batch.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = OutboxRepository(session)
                pending_events = await repo.fetch_pending_events(batch_size=self.batch_size)

                if not pending_events:
                    return 0

                logger.info(f"Claimed {len(pending_events)} pending outbox event(s) for publishing.")

                for event in pending_events:
                    topic = event.event_type
                    # Partition by session_id to guarantee chronological order within partition
                    partition_key = str(event.aggregate_id)

                    try:
                        await self.producer.publish(
                            topic=topic,
                            key=partition_key,
                            value=event.payload,
                            event_id=event.event_id,
                        )
                        await repo.mark_published(event.event_id)
                        self.published_count += 1
                        logger.info(
                            f"[Outbox] Marked event {event.event_id} (type: {topic}) as PUBLISHED."
                        )

                    except Exception as exc:
                        error_msg = str(exc)
                        logger.error(
                            f"[Outbox] Delivery failure for event {event.event_id}: {error_msg}"
                        )
                        await repo.record_failure(
                            event_id=event.event_id,
                            error=error_msg,
                            max_retries=self.max_retries,
                        )
                        self.failed_count += 1

                return len(pending_events)

    async def run_loop(self) -> None:
        """Continuous polling loop."""
        await self.start()
        logger.info("Outbox publisher polling loop running.")

        while self._running:
            try:
                self.poll_cycles += 1
                count = await self.publish_pending_batch()

                if count == 0:
                    # No pending events, wait for the poll interval or shutdown event
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self.poll_interval,
                        )
                        break
                    except asyncio.TimeoutError:
                        continue

            except asyncio.CancelledError:
                logger.info("Outbox publisher loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Unexpected error in outbox publisher loop: {e}", exc_info=True)
                await asyncio.sleep(min(self.poll_interval * 2, 5.0))

        await self.stop()
