"""Agent Trigger Controller for real-time conversation suggestion orchestration.

Consumes conversation.transcript.final events, evaluates conversation progression,
enforces debouncing, minimum message count, session cooldown, and single-run concurrency,
and emits AgentCommandEvent to agent.commands.
"""

import asyncio
from enum import Enum
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.kafka_producer import AIOKafkaEventProducer
from app.database.repository import (
    AgentExecutionRepository,
    ProcessedEventRepository,
)
from app.database.session import async_session_factory
from app.events.commands import AgentCommandEvent, AgentCommandPayload
from app.events.dlq import DLQEvent, DLQPayload
from app.events.speech_activity import SpeechActivityEvent
from app.events.topics import KafkaTopics
from app.events.transcript import TranscriptFinalEvent

logger = logging.getLogger(__name__)


class TurnState(str, Enum):
    """Conversational turn progression states for a session."""

    IDLE = "IDLE"
    SPEAKING = "SPEAKING"
    UTTERANCE_PAUSED = "UTTERANCE_PAUSED"
    TURN_COMPLETED = "TURN_COMPLETED"


@dataclass
class SessionTriggerState:
    """In-memory tracking state for a conversation session."""

    session_id: UUID
    last_processed_sequence: int = 0
    new_messages_count: int = 0
    last_trigger_timestamp: float = 0.0
    debounce_task: Optional[asyncio.Task] = None

    # Gate 1: Turn-Completion State Machine
    turn_state: TurnState = TurnState.IDLE
    last_activity_sequence: int = 0
    last_activity_timestamp: float = 0.0
    last_speech_onset_timestamp: float = 0.0
    last_speech_onset_source_sequence: int = 0
    last_speech_onset_last_finalized_sequence: int = 0
    turn_timer_task: Optional[asyncio.Task] = None
    last_utterance_text: str = ""
    last_utterance_end_time: float = 0.0




class AgentTriggerController:
    """Controller monitoring conversation flow and dispatching agent execution triggers.

    Orchestration Rules:
    1. Min New Messages: Requires at least AGENT_TRIGGER_MIN_NEW_MESSAGES (e.g. 3)
       new transcript utterances before evaluating a trigger.
    2. Debounce: Pauses AGENT_TRIGGER_DEBOUNCE_MS (e.g. 3000ms) after the latest
       message to ensure natural pauses before triggering LLM inference.
    3. Cooldown: Enforces AGENT_SESSION_COOLDOWN_SECONDS (e.g. 20s) between consecutive
       triggers for the same session to avoid rapid-fire inference.
    4. Concurrency Limit: Verifies no active running execution exists for the session
       (AGENT_MAX_CONCURRENT_RUNS_PER_SESSION=1).
    5. Safe Default: If AGENT_AUTO_RUN is False, no commands are dispatched automatically.
    """

    CONSUMER_ID = "trigger_controller"

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = "conversa-trigger-controllers",
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        producer: Optional[AIOKafkaEventProducer] = None,
        dlq_producer: Optional[AIOKafkaEventProducer] = None,
        min_new_messages: Optional[int] = None,
        debounce_ms: Optional[int] = None,
        cooldown_seconds: Optional[float] = None,
        max_concurrent_runs: Optional[int] = None,
        auto_run: Optional[bool] = None,
        auto_offset_reset: str = "earliest",
        max_retries: int = 3,
        turn_completion_enabled: Optional[bool] = None,
        turn_pause_question_ms: Optional[int] = None,
        turn_pause_complete_ms: Optional[int] = None,
        turn_pause_incomplete_ms: Optional[int] = None,
    ) -> None:
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.group_id = group_id
        self.session_factory = session_factory or async_session_factory()
        self.min_new_messages = (
            min_new_messages
            if min_new_messages is not None
            else settings.agent_trigger_min_new_messages
        )
        self.debounce_ms = (
            debounce_ms
            if debounce_ms is not None
            else settings.agent_trigger_debounce_ms
        )
        self.cooldown_seconds = (
            cooldown_seconds
            if cooldown_seconds is not None
            else settings.agent_session_cooldown_seconds
        )
        self.max_concurrent_runs = (
            max_concurrent_runs
            if max_concurrent_runs is not None
            else settings.agent_max_concurrent_runs_per_session
        )
        self.auto_run = (
            auto_run if auto_run is not None else settings.agent_auto_run
        )
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries

        # Gate 1 Turn-Completion settings
        self.turn_completion_enabled = (
            turn_completion_enabled
            if turn_completion_enabled is not None
            else settings.agent_turn_completion_enabled
        )
        self.turn_pause_question_ms = (
            turn_pause_question_ms
            if turn_pause_question_ms is not None
            else settings.agent_turn_pause_question_ms
        )
        self.turn_pause_complete_ms = (
            turn_pause_complete_ms
            if turn_pause_complete_ms is not None
            else settings.agent_turn_pause_complete_ms
        )
        self.turn_pause_incomplete_ms = (
            turn_pause_incomplete_ms
            if turn_pause_incomplete_ms is not None
            else settings.agent_turn_pause_incomplete_ms
        )

        self.producer = producer or AIOKafkaEventProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id="conversa-trigger-controller-producer",
        )
        self.dlq_producer = dlq_producer or AIOKafkaEventProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id="conversa-trigger-controller-dlq",
        )

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._consume_task: Optional[asyncio.Task] = None

        # Session state tracking
        self._session_states: Dict[UUID, SessionTriggerState] = {}
        self._lock = asyncio.Lock()

        # Telemetry metrics
        self.events_received = 0
        self.triggers_emitted = 0
        self.triggers_skipped_cooldown = 0
        self.triggers_skipped_concurrency = 0
        self.duplicates_skipped = 0
        self.dlq_count = 0
        self.speech_activity_events_received = 0
        self.turn_timers_cancelled = 0
        self.stale_speech_events_skipped = 0
        self.turns_completed = 0

    def get_or_create_session_state(self, session_id: UUID) -> SessionTriggerState:
        if session_id not in self._session_states:
            self._session_states[session_id] = SessionTriggerState(session_id=session_id)
        return self._session_states[session_id]

    async def start(self) -> None:
        """Start the controller Kafka consumer and event producer."""
        logger.info(
            "Starting AgentTriggerController [group=%s, auto_run=%s, min_msgs=%d, debounce_ms=%d, turn_completion=%s]...",
            self.group_id,
            self.auto_run,
            self.min_new_messages,
            self.debounce_ms,
            self.turn_completion_enabled,
        )
        await self.producer.start()
        await self.dlq_producer.start()

        topics = [KafkaTopics.TRANSCRIPT_FINAL.value]
        if self.turn_completion_enabled:
            topics.append(KafkaTopics.SPEECH_ACTIVITY.value)

        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        await self._consumer.start()
        self._running = True
        self._shutdown_event.clear()
        self._consume_task = asyncio.create_task(self._consume_loop())
        logger.info("AgentTriggerController started listening on %s.", topics)

    async def stop(self) -> None:
        """Stop consumer, cancel debouncers and turn timers, and flush producer."""
        logger.info("Stopping AgentTriggerController...")
        self._running = False
        self._shutdown_event.set()

        # Cancel any active debounce and turn timer tasks
        async with self._lock:
            for state in self._session_states.values():
                if state.debounce_task and not state.debounce_task.done():
                    state.debounce_task.cancel()
                if state.turn_timer_task and not state.turn_timer_task.done():
                    state.turn_timer_task.cancel()

        if self._consume_task and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass

        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning("Error stopping consumer: %s", e)

        await self.producer.stop()
        await self.dlq_producer.stop()
        logger.info("AgentTriggerController stopped.")


    async def run_loop(self) -> None:
        """Run the controller until shutdown signal is received."""
        await self.start()
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _consume_loop(self) -> None:
        """Main Kafka consumption loop with manual offset commit."""
        while self._running:
            try:
                if self._consumer is None:
                    break
                records = await self._consumer.getmany(timeout_ms=1000, max_records=20)
                for topic_partition, record_list in records.items():
                    for record in record_list:
                        if not self._running:
                            break
                        processed = await self.process_record(record)
                        if processed:
                            await self._consumer.commit({topic_partition: record.offset + 1})
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Unexpected error in trigger controller consume loop: %s", exc)
                await asyncio.sleep(1.0)

    async def send_to_dlq(
        self,
        record: ConsumerRecord,
        error_stage: str,
        error_message: str,
        raw_payload: Any = None,
        event_id: Optional[str] = None,
        session_id: Optional[UUID] = None,
    ) -> None:
        """Publish malformed or unprocessable record to DLQ."""
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
                "[Trigger Controller DLQ] Routed failed record to %s (stage=%s)",
                KafkaTopics.AGENT_DLQ.value,
                error_stage,
            )
        except Exception as e:
            logger.critical("[Trigger Controller DLQ] Failed to route to DLQ: %s", e)

    async def process_record(self, record: ConsumerRecord) -> bool:
        """Parse, deduplicate, and process a transcript or speech activity event record.

        Returns:
            True if processed, skipped as duplicate, or routed to DLQ.
        """
        self.events_received += 1
        raw_val = record.value

        # 1. Deserialize JSON
        try:
            data = json.loads(raw_val)
        except Exception as exc:
            logger.error("[Trigger Controller] JSON parse error: %s", exc)
            await self.send_to_dlq(
                record=record,
                error_stage="json_deserialization",
                error_message=str(exc),
                raw_payload=raw_val[:1000] if raw_val else None,
            )
            return True

        # Check topic or event_type to branch between speech.activity and transcript.final
        topic = getattr(record, "topic", "")
        event_type = data.get("event_type") if isinstance(data, dict) else ""

        if topic == KafkaTopics.SPEECH_ACTIVITY.value or event_type == KafkaTopics.SPEECH_ACTIVITY.value:
            return await self.process_speech_activity_record(record, data)

        # 2. Validate transcript.final schema
        try:
            event = TranscriptFinalEvent.model_validate(data)
        except Exception as exc:
            logger.error("[Trigger Controller] Schema validation error: %s", exc)
            event_id = data.get("event_id") if isinstance(data, dict) else None
            await self.send_to_dlq(
                record=record,
                error_stage="schema_validation",
                error_message=str(exc),
                raw_payload=data,
                event_id=str(event_id) if event_id else None,
            )
            return True

        # 3. Deduplication check via ProcessedEventRepository
        event_id = event.event_id
        session_id = event.session_id
        seq_num = event.sequence_number

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.session_factory() as session:
                    async with session.begin():
                        repo = ProcessedEventRepository(session)
                        if await repo.is_processed(event_id, consumer_id=self.CONSUMER_ID):
                            self.duplicates_skipped += 1
                            logger.info(
                                "[Trigger Controller] Event %s already processed by %s. Skipping.",
                                event_id,
                                self.CONSUMER_ID,
                            )
                            return True

                        await repo.mark_processed(
                            event_id=event_id,
                            event_type=event.event_type,
                            session_id=session_id,
                            consumer_id=self.CONSUMER_ID,
                        )

                # Event state successfully marked in DB
                await self.on_transcript_event(
                    session_id=session_id,
                    sequence_number=seq_num,
                    text=event.payload.text,
                    occurred_at=event.occurred_at,
                    source_sequence=event.payload.source_sequence,
                    associated_activity_sequence=event.payload.associated_activity_sequence,
                )
                return True

            except Exception as exc:
                logger.warning(
                    "[Trigger Controller] Transient DB error (attempt %d/%d): %s",
                    attempt,
                    self.max_retries,
                    exc,
                )
                if attempt == self.max_retries:
                    logger.error("[Trigger Controller] Max retries exhausted for event %s", event_id)
                    await self.send_to_dlq(
                        record=record,
                        error_stage="database_commit",
                        error_message=str(exc),
                        raw_payload=data,
                        event_id=str(event_id),
                        session_id=session_id,
                    )
                    return True
                await asyncio.sleep(0.2 * (2 ** (attempt - 1)))

        return True

    async def process_speech_activity_record(
        self, record: ConsumerRecord, data: Dict[str, Any]
    ) -> bool:
        """Parse and handle a speech.activity event."""
        try:
            event = SpeechActivityEvent.model_validate(data)
        except Exception as exc:
            logger.error("[Trigger Controller] Speech activity schema validation error: %s", exc)
            event_id = data.get("event_id") if isinstance(data, dict) else None
            await self.send_to_dlq(
                record=record,
                error_stage="schema_validation",
                error_message=str(exc),
                raw_payload=data,
                event_id=str(event_id) if event_id else None,
            )
            return True

        return await self.on_speech_activity_event(
            session_id=event.session_id,
            event_id=event.event_id,
            activity_sequence=event.payload.activity_sequence,
            timestamp=event.payload.timestamp,
            activity_type=event.payload.activity_type,
            source_sequence=event.payload.source_sequence,
            last_finalized_sequence=event.payload.last_finalized_sequence,
        )

    async def on_speech_activity_event(
        self,
        session_id: UUID,
        event_id: UUID,
        activity_sequence: int,
        timestamp: float,
        activity_type: str = "speech_onset",
        source_sequence: Optional[int] = None,
        last_finalized_sequence: Optional[int] = None,
    ) -> bool:
        """Handle incoming speech activity event to cancel pending turn timers and update state."""
        self.speech_activity_events_received += 1
        async with self._lock:
            state = self.get_or_create_session_state(session_id)

            # Deduplication & Stale Detection Rule 1: Monotonic activity sequence
            if activity_sequence <= state.last_activity_sequence:
                self.stale_speech_events_skipped += 1
                logger.info(
                    "[Trigger Controller] Stale/duplicate speech activity event (seq=%d <= last=%d) for session %s. Discarding.",
                    activity_sequence,
                    state.last_activity_sequence,
                    session_id,
                )
                return True

            # Deduplication & Stale Detection Rule 2: Timestamp ordering check
            if state.last_activity_timestamp > 0 and timestamp < state.last_activity_timestamp:
                self.stale_speech_events_skipped += 1
                logger.info(
                    "[Trigger Controller] Out-of-order stale speech activity timestamp (ts=%.3f < last=%.3f) for session %s. Discarding.",
                    timestamp,
                    state.last_activity_timestamp,
                    session_id,
                )
                return True

            # Valid speech onset transition!
            state.last_activity_sequence = activity_sequence
            state.last_activity_timestamp = max(state.last_activity_timestamp, timestamp)
            state.last_speech_onset_timestamp = max(state.last_speech_onset_timestamp, timestamp)
            if source_sequence is not None:
                state.last_speech_onset_source_sequence = max(
                    state.last_speech_onset_source_sequence, source_sequence
                )
            if last_finalized_sequence is not None:
                state.last_speech_onset_last_finalized_sequence = max(
                    state.last_speech_onset_last_finalized_sequence, last_finalized_sequence
                )
            state.turn_state = TurnState.SPEAKING

            # Cancel pending turn completion timer immediately!
            if state.turn_timer_task and not state.turn_timer_task.done():
                state.turn_timer_task.cancel()
                state.turn_timer_task = None
                self.turn_timers_cancelled += 1
                logger.info(
                    "[Trigger Controller] CANCELLED pending turn-completion timer for session %s due to speech resumption (seq=%d).",
                    session_id,
                    activity_sequence,
                )

            # Cancel debounce task if running
            if state.debounce_task and not state.debounce_task.done():
                state.debounce_task.cancel()
                state.debounce_task = None
                logger.info(
                    "[Trigger Controller] CANCELLED debounce timer for session %s due to speech resumption.",
                    session_id,
                )

            return True

    def is_actionable_question(self, text: str) -> bool:
        """Determine if an utterance represents a completed actionable question.

        Completed actionable questions end with '?' or start with question words
        without trailing hesitation/conjunctions indicating incomplete thoughts.
        """
        clean = text.strip() if text else ""
        if not clean:
            return False

        # Hesitation or trailing conjunctions disqualify as completed question
        if clean.endswith(",") or clean.endswith("...") or clean.endswith("…"):
            return False

        lower = clean.lower()
        trailing_conjunctions = (
            " and", " but", " or", " so", " because", " though",
            " although", " that", " like", " if", " while",
        )
        if any(lower.endswith(conj) for conj in trailing_conjunctions):
            return False

        # 1. Ends with question mark
        if clean.endswith("?"):
            return True

        # 2. Interrogative phrasing starter within concise conversational question length
        question_starters = (
            "what", "why", "how", "when", "where", "who", "which",
            "is it", "can we", "could you", "do you", "should we", "would you",
            "are you", "does anyone", "has anyone",
        )
        if any(lower.startswith(q) for q in question_starters) and len(clean.split()) <= 15:
            return True

        return False

    async def on_transcript_event(
        self,
        session_id: UUID,
        sequence_number: int,
        text: str = "",
        occurred_at: Optional[datetime] = None,
        source_sequence: Optional[int] = None,
        associated_activity_sequence: Optional[int] = None,
    ) -> None:
        """Handle newly committed transcript message for session trigger tracking."""
        async with self._lock:
            state = self.get_or_create_session_state(session_id)
            state.new_messages_count += 1
            state.last_processed_sequence = max(state.last_processed_sequence, sequence_number)
            state.last_utterance_text = text
            state.last_utterance_end_time = time.monotonic()

            # Always cancel any pending turn timer when new speech or utterance arrives
            if state.turn_timer_task and not state.turn_timer_task.done():
                state.turn_timer_task.cancel()
                state.turn_timer_task = None

            # Always cancel any pending debounce timer
            if state.debounce_task and not state.debounce_task.done():
                state.debounce_task.cancel()
                state.debounce_task = None

            # Cross-Topic Kafka Ordering Guard:
            # Check if speaker has already resumed speaking a subsequent utterance
            is_subsequent_speaking = False
            if state.turn_state == TurnState.SPEAKING:
                if state.last_speech_onset_last_finalized_sequence >= sequence_number:
                    is_subsequent_speaking = True
                elif (
                    associated_activity_sequence is not None
                    and state.last_activity_sequence > associated_activity_sequence
                ):
                    is_subsequent_speaking = True
                elif occurred_at is not None:
                    occ_ts = occurred_at.timestamp() if hasattr(occurred_at, "timestamp") else 0.0
                    if state.last_speech_onset_timestamp > (occ_ts + 0.05):
                        is_subsequent_speaking = True

            if is_subsequent_speaking:
                logger.info(
                    "[Trigger Controller] Transcript arrived for finalized seq=%d, but participant has already resumed speaking (activity_seq=%d, last_finalized=%d). State remains SPEAKING.",
                    sequence_number,
                    state.last_activity_sequence,
                    state.last_speech_onset_last_finalized_sequence,
                )
                return

            # Participant paused speaking after completing this utterance
            state.turn_state = TurnState.UTTERANCE_PAUSED

            if not self.auto_run:
                logger.debug(
                    "[Trigger Controller] AGENT_AUTO_RUN is disabled. Recorded message %d for session %s.",
                    sequence_number,
                    session_id,
                )
                return

            # Threshold Check:
            # 1. In turn-completion mode, a completed actionable question qualifies immediately (1 message).
            # 2. Incomplete or declarative utterances require at least min_new_messages (e.g. 3).
            is_question = self.is_actionable_question(text) if self.turn_completion_enabled else False
            threshold_satisfied = (
                state.new_messages_count >= self.min_new_messages
                or (self.turn_completion_enabled and is_question)
            )

            if not threshold_satisfied:
                logger.debug(
                    "[Trigger Controller] Session %s has %d/%d new messages (is_question=%s). Waiting for threshold.",
                    session_id,
                    state.new_messages_count,
                    self.min_new_messages,
                    is_question,
                )
                return

            # Gate 1: Conversational Turn-Completion Intelligence
            if self.turn_completion_enabled:
                pause_ms = self.classify_turn_pause(text)
                logger.info(
                    "[Trigger Controller] Gate 1 Turn Completion active for session %s: classified pause = %d ms (text='%s')",
                    session_id,
                    pause_ms,
                    text[:40] if text else "",
                )

                state.turn_timer_task = asyncio.create_task(
                    self._wait_for_turn_completion(
                        session_id=session_id,
                        seq_number=state.last_processed_sequence,
                        messages_count=state.new_messages_count,
                        pause_ms=pause_ms,
                    )
                )
                return

            # Traditional debounce fallback
            if self.debounce_ms <= 0:
                await self._evaluate_and_dispatch_internal(
                    session_id=session_id,
                    seq_number=state.last_processed_sequence,
                    messages_count=state.new_messages_count,
                    trigger_reason="min_messages_immediate",
                )
                return

            # Schedule debounce execution
            state.debounce_task = asyncio.create_task(
                self._debounce_and_evaluate(
                    session_id=session_id,
                    seq_number=state.last_processed_sequence,
                    messages_count=state.new_messages_count,
                )
            )

    def classify_turn_pause(self, text: str) -> int:
        """Classify pause threshold based on syntactic and lexical turn completion cues."""
        clean = text.strip() if text else ""
        if not clean:
            return self.turn_pause_complete_ms

        # 1. Question completion: Ends with '?' or starts with interrogative phrasing
        if clean.endswith("?"):
            return self.turn_pause_question_ms

        lower = clean.lower()
        question_starters = (
            "what", "why", "how", "when", "where", "who", "which",
            "is it", "can we", "could you", "do you", "should we", "would you",
            "are you", "does anyone", "has anyone",
        )
        if any(lower.startswith(q) for q in question_starters) and len(clean.split()) <= 15:
            # Check trailing hesitation
            if not (clean.endswith(",") or clean.endswith("...") or clean.endswith("…")):
                return self.turn_pause_question_ms

        # 2. Incomplete thought: Trailing comma, ellipsis, dash, or conjunction
        if clean.endswith(",") or clean.endswith("...") or clean.endswith("…"):
            return self.turn_pause_incomplete_ms

        trailing_conjunctions = (
            " and", " but", " or", " so", " because", " though",
            " although", " that", " like", " if", " while",
        )
        for conj in trailing_conjunctions:
            if lower.endswith(conj):
                return self.turn_pause_incomplete_ms

        # 3. Complete declarative sentence
        return self.turn_pause_complete_ms

    async def _wait_for_turn_completion(
        self, session_id: UUID, seq_number: int, messages_count: int, pause_ms: int
    ) -> None:
        """Wait for turn pause threshold without speech resumption, then evaluate trigger."""
        current_task = asyncio.current_task()
        try:
            if pause_ms > 0:
                await asyncio.sleep(pause_ms / 1000.0)

            async with self._lock:
                state = self.get_or_create_session_state(session_id)

                # Stale Timer Guard 1: Verify timer task was not cancelled or superseded
                if state.turn_timer_task is not current_task:
                    logger.debug(
                        "[Trigger Controller] Turn timer task superseded or cancelled for session %s.",
                        session_id,
                    )
                    return

                # Stale Timer Guard 2: Verify participant is not actively speaking
                if state.turn_state == TurnState.SPEAKING:
                    logger.info(
                        "[Trigger Controller] Turn timer ignored for session %s: participant is actively speaking.",
                        session_id,
                    )
                    return

                # Stale Timer Guard 3: Verify no newer sequence number arrived during pause
                if state.last_processed_sequence != seq_number:
                    logger.info(
                        "[Trigger Controller] Turn timer ignored for session %s: seq %d != latest %d.",
                        session_id,
                        seq_number,
                        state.last_processed_sequence,
                    )
                    return

                state.turn_state = TurnState.TURN_COMPLETED
                self.turns_completed += 1

            await self.evaluate_and_dispatch(
                session_id=session_id,
                seq_number=seq_number,
                messages_count=messages_count,
                trigger_reason="turn_completed",
            )
        except asyncio.CancelledError:
            logger.debug(
                "[Trigger Controller] Turn completion timer cancelled for session %s (speech resumed).",
                session_id,
            )
        except Exception as exc:
            logger.error(
                "[Trigger Controller] Error during turn completion evaluation: %s", exc
            )


    async def _debounce_and_evaluate(
        self, session_id: UUID, seq_number: int, messages_count: int
    ) -> None:
        """Wait for conversation pause (debounce) then evaluate trigger criteria."""
        try:
            if self.debounce_ms > 0:
                await asyncio.sleep(self.debounce_ms / 1000.0)

            await self.evaluate_and_dispatch(
                session_id=session_id,
                seq_number=seq_number,
                messages_count=messages_count,
                trigger_reason="min_messages_debounce",
            )
        except asyncio.CancelledError:
            logger.debug("[Trigger Controller] Debounce cancelled for session %s (new utterance arrived).", session_id)
        except Exception as exc:
            logger.error("[Trigger Controller] Error during debounce evaluation: %s", exc)

    async def evaluate_and_dispatch(
        self,
        session_id: UUID,
        seq_number: int,
        messages_count: int,
        trigger_reason: str = "auto_trigger",
        force: bool = False,
    ) -> bool:
        """Evaluate cooldown and concurrency constraints under lock, then dispatch."""
        async with self._lock:
            return await self._evaluate_and_dispatch_internal(
                session_id=session_id,
                seq_number=seq_number,
                messages_count=messages_count,
                trigger_reason=trigger_reason,
                force=force,
            )

    async def _evaluate_and_dispatch_internal(
        self,
        session_id: UUID,
        seq_number: int,
        messages_count: int,
        trigger_reason: str = "auto_trigger",
        force: bool = False,
    ) -> bool:
        """Internal dispatch logic executing while self._lock is held."""
        state = self.get_or_create_session_state(session_id)
        now = time.monotonic()

        # Turn completion state check: abort if participant resumed speaking
        if self.turn_completion_enabled and state.turn_state == TurnState.SPEAKING:
            logger.info(
                "[Trigger Controller] Dispatch aborted for session %s: participant resumed speaking.",
                session_id,
            )
            return False

        # 1. Cooldown Check
        if not force and self.cooldown_seconds > 0:
            elapsed = now - state.last_trigger_timestamp
            if state.last_trigger_timestamp > 0 and elapsed < self.cooldown_seconds:
                self.triggers_skipped_cooldown += 1
                logger.info(
                    "[Trigger Controller] Cooldown active for session %s (%.1fs < %.1fs). Skipping trigger.",
                    session_id,
                    elapsed,
                    self.cooldown_seconds,
                )
                return False

        # 2. Concurrency Check
        if not force:
            async with self.session_factory() as session:
                exec_repo = AgentExecutionRepository(session)
                has_active = await exec_repo.has_active_execution(session_id)
                if has_active:
                    self.triggers_skipped_concurrency += 1
                    logger.info(
                        "[Trigger Controller] Session %s already has an active agent execution. Skipping trigger.",
                        session_id,
                    )
                    return False

        # 3. Dispatch AgentCommandEvent
        command_payload = AgentCommandPayload(
            session_id=session_id,
            trigger_reason=trigger_reason,
            processed_sequence_number=seq_number,
            message_count=messages_count,
        )
        command_event = AgentCommandEvent(
            session_id=session_id,
            payload=command_payload,
        )

        await self.producer.publish(
            topic=KafkaTopics.AGENT_COMMANDS.value,
            key=str(session_id),
            value=command_event.model_dump(mode="json"),
            event_id=command_event.event_id,
        )

        state.last_trigger_timestamp = now
        state.new_messages_count = 0
        self.triggers_emitted += 1

        logger.info(
            "[Trigger Controller] Dispatched command to %s for session %s (seq=%d, reason=%s).",
            KafkaTopics.AGENT_COMMANDS.value,
            session_id,
            seq_number,
            trigger_reason,
        )
        return True
