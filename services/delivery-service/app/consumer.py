"""Delivery Service consumer handlers and Kafka listener.

Receives finalized transcript events and generated agent suggestions from Kafka,
and delivers them in real time over WebSockets with strict session isolation.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set
from uuid import UUID

try:
    from app.websocket import DeliveryConnectionManager
except ImportError:
    try:
        from websocket import DeliveryConnectionManager
    except ImportError:
        from .websocket import DeliveryConnectionManager

logger = logging.getLogger(__name__)


class KafkaDeliveryConsumer:
    """Kafka consumer streaming committed transcripts and agent suggestions to WebSockets."""

    def __init__(
        self,
        manager: DeliveryConnectionManager,
        bootstrap_servers: Optional[str] = None,
        topics: Optional[List[str]] = None,
        group_id: str = "conversa-delivery-workers",
    ) -> None:
        self.manager = manager
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )
        self.topics = topics or [
            "conversation.transcript.final",
            "agent.suggestions",
        ]
        self.group_id = group_id
        self._consumer = None
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # Metrics
        self.messages_delivered = 0
        self._processed_event_ids: Set[str] = set()

    async def start(self) -> None:
        """Start background Kafka consumer."""
        try:
            from aiokafka import AIOKafkaConsumer

            self._consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            await self._consumer.start()
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info("KafkaDeliveryConsumer started on topics %s", self.topics)
        except Exception as exc:
            logger.warning(
                "Could not start Kafka consumer in Delivery Service: %s (falling back to direct HTTP delivery)",
                exc,
            )

    async def process_event_payload(self, topic: str, data: Dict[str, Any]) -> int:
        """Parse event payload and route to connected session clients.

        Returns number of clients to which message was broadcast.
        """
        event_type = data.get("event_type") or topic

        if topic == "agent.suggestions" or event_type == "agent.suggestions":
            # Unpack suggestion payload
            inner = data.get("payload", {})
            suggestion_id = inner.get("suggestion_id") or data.get("event_id")

            # Idempotency check: prevent repeated delivery of the same suggestion event
            if suggestion_id:
                sugg_id_str = str(suggestion_id)
                if sugg_id_str in self._processed_event_ids:
                    logger.info(
                        "[Delivery Service] Duplicate suggestion event %s already broadcast. Skipping.",
                        sugg_id_str,
                    )
                    return 0
                self._processed_event_ids.add(sugg_id_str)
                if len(self._processed_event_ids) > 10000:
                    self._processed_event_ids.pop()

            session_id = (
                inner.get("session_id")
                or data.get("aggregate_id")
                or data.get("session_id")
            )
            if not session_id:
                logger.warning("Agent suggestion event missing session_id: %s", data)
                return 0

            event_body = {
                "type": "suggestion",
                "session_id": str(session_id),
                "suggestion_id": str(inner.get("suggestion_id", "")),
                "response_type": inner.get("response_type", "perspective"),
                "text": inner.get("suggestion_text") or inner.get("text", ""),
                "rationale": inner.get("rationale"),
                "source_sequence_number": inner.get("source_sequence_number"),
                "timestamp": data.get("timestamp") or inner.get("created_at"),
            }
            delivered = await self.manager.broadcast_to_session(str(session_id), event_body)
            self.messages_delivered += delivered
            logger.info(
                "[Delivery Service] Broadcast suggestion %s to session %s (%d clients)",
                event_body["suggestion_id"],
                session_id,
                delivered,
            )
            return delivered

        elif topic == "conversation.transcript.final" or event_type == "conversation.transcript.final":
            event_id = data.get("event_id")
            if event_id:
                ev_id_str = str(event_id)
                if ev_id_str in self._processed_event_ids:
                    return 0
                self._processed_event_ids.add(ev_id_str)
                if len(self._processed_event_ids) > 10000:
                    self._processed_event_ids.pop()

            session_id = data.get("aggregate_id") or data.get("session_id")
            if session_id:
                event_body = {
                    "type": "final_from_kafka",
                    "session_id": str(session_id),
                    "event_id": data.get("event_id"),
                    "data": data.get("payload", {}),
                }
                delivered = await self.manager.broadcast_to_session(str(session_id), event_body)
                self.messages_delivered += delivered
                return delivered

        return 0

    async def _consume_loop(self) -> None:
        try:
            async for msg in self._consumer:
                if not self._running:
                    break
                try:
                    payload = json.loads(msg.value.decode("utf-8"))
                    await self.process_event_payload(msg.topic, payload)
                except Exception as exc:
                    logger.error("Error processing Kafka message in Delivery Service: %s", exc)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Kafka consumer loop error: %s", exc)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            logger.info("KafkaDeliveryConsumer stopped")
