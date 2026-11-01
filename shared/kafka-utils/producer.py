"""Reusable asynchronous Kafka producer with delivery acknowledgement and retry handling."""

import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class KafkaPublishError(Exception):
    """Raised when publishing an event to Kafka fails."""

    def __init__(self, message: str, event_id: str | None = None, topic: str | None = None):
        super().__init__(message)
        self.event_id = event_id
        self.topic = topic


class AIOKafkaEventProducer:
    """Production-grade asynchronous Kafka producer for Conversa AI event streaming.

    Features:
    - Guaranteed delivery acknowledgement (acks='all').
    - Strong ordering when messages are partitioned by key (e.g. session_id).
    - Robust retry handling for transient broker/network disruptions.
    - Clean lifecycle management (start/stop/context manager).
    - Structured logging with event_id, omitting sensitive secrets.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str = "conversa-producer",
        acks: str = "all",
        retries: int = 5,
        retry_backoff_ms: int = 200,
        request_timeout_ms: int = 15000,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.acks = acks
        self.retries = retries
        self.retry_backoff_ms = retry_backoff_ms
        self.request_timeout_ms = request_timeout_ms

        self._producer: AIOKafkaProducer | None = None
        self._is_started = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the internal AIOKafkaProducer instance."""
        async with self._lock:
            if self._is_started and self._producer is not None:
                return

            logger.info(
                f"Starting Kafka producer for servers: {self.bootstrap_servers} (client_id: {self.client_id})"
            )
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                acks=self.acks,
                retry_backoff_ms=self.retry_backoff_ms,
                request_timeout_ms=self.request_timeout_ms,
                enable_idempotence=True,  # Idempotent producer guarantees no duplicates on network retries
            )

            await self._producer.start()
            self._is_started = True
            logger.info("Kafka producer successfully started and connected to broker.")

    async def stop(self) -> None:
        """Gracefully flush pending batches and stop the producer."""
        async with self._lock:
            if not self._is_started or self._producer is None:
                return

            logger.info("Stopping Kafka producer and flushing pending messages...")
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning(f"Error stopping Kafka producer: {e}")
            finally:
                self._producer = None
                self._is_started = False
                logger.info("Kafka producer stopped.")

    async def __aenter__(self) -> "AIOKafkaEventProducer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    def serialize_value(self, value: Any) -> bytes:
        """Serialize payload into UTF-8 JSON bytes."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        if isinstance(value, BaseModel):
            return value.model_dump_json().encode("utf-8")
        if isinstance(value, dict):
            return json.dumps(value, default=str).encode("utf-8")
        raise TypeError(f"Unsupported event payload type: {type(value)}")

    async def publish(
        self,
        topic: str,
        key: str | UUID | None,
        value: Any,
        event_id: str | UUID | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> RecordMetadata:
        """Publish an event to a Kafka topic and await broker delivery acknowledgement.

        Args:
            topic: Target Kafka topic.
            key: Partition key (e.g. session_id string) ensuring chronological partition ordering.
            value: Event payload (BaseModel, dict, or string).
            event_id: Unique event UUID for logging and tracing.
            headers: Optional Kafka record headers.

        Returns:
            RecordMetadata with topic, partition, and committed offset.
        """
        if not self._is_started or self._producer is None:
            await self.start()

        key_bytes = str(key).encode("utf-8") if key is not None else None
        value_bytes = self.serialize_value(value)
        event_id_str = str(event_id) if event_id is not None else "unknown"

        try:
            metadata: RecordMetadata = await self._producer.send_and_wait(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers,
            )
            logger.info(
                f"Successfully published event [{event_id_str}] to topic '{topic}' "
                f"[partition={metadata.partition}, offset={metadata.offset}]"
            )
            return metadata

        except Exception as exc:
            logger.error(
                f"Failed to publish event [{event_id_str}] to topic '{topic}': {exc}"
            )
            raise KafkaPublishError(
                message=f"Failed to publish event to {topic}: {exc}",
                event_id=event_id_str,
                topic=topic,
            ) from exc
