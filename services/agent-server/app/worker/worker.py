"""Independent Agent Worker process.

Consumes agent.commands events, validates triggers, checks for staleness,
retrieves session context from PostgreSQL, executes the LangGraph suggestion pipeline
(or mock pipeline in budget-protected testing mode), and atomically writes suggestions
and transactional outbox events to PostgreSQL.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.agent.graph import get_agent_graph
from app.core.config import get_settings
from app.core.kafka_producer import AIOKafkaEventProducer
from app.core.parsing import StructuredOutputError
from app.database.repository import (
    AgentExecutionRepository,
    ConversationRepository,
    ProcessedEventRepository,
    SessionNotFoundError,
    SessionRepository,
    SuggestionRepository,
)
from app.database.session import async_session_factory
from app.events.commands import AgentCommandEvent
from app.events.dlq import DLQEvent, DLQPayload
from app.events.topics import KafkaTopics

logger = logging.getLogger(__name__)


class AgentWorker:
    """Scalable worker executing conversational intelligence inference upon command.

    Guarantees:
    - Independent Scalability: Runs as an autonomous worker process listening to agent.commands.
    - Idempotency & Deduplication: Prevents re-running identical command event IDs.
    - Staleness Filtering: Drops commands where sequence <= latest completed execution.
    - Transactional Outbox: Generated suggestions and outbox events are committed in the same DB transaction.
    - Budget-Protection Mock Mode: Respects MOCK_LLM=true so automated tests never consume OpenRouter credits.
    """

    CONSUMER_ID = "agent_worker"

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = "conversa-agent-workers",
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        dlq_producer: Optional[AIOKafkaEventProducer] = None,
        mock_llm: Optional[bool] = None,
        auto_offset_reset: str = "earliest",
        max_retries: int = 3,
    ) -> None:
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.group_id = group_id
        self.session_factory = session_factory or async_session_factory()
        self.mock_llm = mock_llm if mock_llm is not None else settings.mock_llm
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries

        self.dlq_producer = dlq_producer or AIOKafkaEventProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id="conversa-agent-worker-dlq",
        )

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._consume_task: Optional[asyncio.Task] = None

        # Telemetry metrics
        self.commands_received = 0
        self.executions_started = 0
        self.executions_completed = 0
        self.executions_no_suggestion = 0
        self.executions_stale_skipped = 0
        self.executions_failed = 0
        self.suggestions_generated = 0
        self.duplicates_skipped = 0
        self.dlq_count = 0

    async def start(self) -> None:
        """Start the worker consumer and DLQ producer."""
        logger.info(
            "Starting AgentWorker [group=%s, mock_llm=%s]...",
            self.group_id,
            self.mock_llm,
        )
        await self.dlq_producer.start()

        self._consumer = AIOKafkaConsumer(
            KafkaTopics.AGENT_COMMANDS.value,
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
        logger.info("AgentWorker started listening on %s.", KafkaTopics.AGENT_COMMANDS.value)

    async def stop(self) -> None:
        """Stop worker consumer and DLQ producer."""
        logger.info("Stopping AgentWorker...")
        self._running = False
        self._shutdown_event.set()

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
                logger.warning("Error stopping agent worker consumer: %s", e)

        await self.dlq_producer.stop()
        logger.info("AgentWorker stopped.")

    async def run_loop(self) -> None:
        """Run the worker until shutdown signal is received."""
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
                records = await self._consumer.getmany(timeout_ms=1000, max_records=10)
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
                logger.error("Unexpected error in agent worker consume loop: %s", exc)
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
                "[Agent Worker DLQ] Routed failed record to %s (stage=%s)",
                KafkaTopics.AGENT_DLQ.value,
                error_stage,
            )
        except Exception as e:
            logger.critical("[Agent Worker DLQ] Failed to route to DLQ: %s", e)

    async def process_record(self, record: ConsumerRecord) -> bool:
        """Process an AgentCommandEvent end-to-end.

        Returns:
            True if processed, skipped, or sent to DLQ (safe to commit offset).
        """
        self.commands_received += 1
        raw_val = record.value

        # 1. Deserialize JSON
        try:
            data = json.loads(raw_val)
        except Exception as exc:
            logger.error("[Agent Worker] JSON parse error: %s", exc)
            await self.send_to_dlq(
                record=record,
                error_stage="json_deserialization",
                error_message=str(exc),
                raw_payload=raw_val[:1000] if raw_val else None,
            )
            return True

        # 2. Validate Schema
        try:
            event = AgentCommandEvent.model_validate(data)
        except Exception as exc:
            logger.error("[Agent Worker] Schema validation error: %s", exc)
            event_id = data.get("event_id") if isinstance(data, dict) else None
            await self.send_to_dlq(
                record=record,
                error_stage="schema_validation",
                error_message=str(exc),
                raw_payload=data,
                event_id=str(event_id) if event_id else None,
            )
            return True

        event_id = event.event_id
        session_id = event.session_id
        cmd_payload = event.payload
        target_seq = cmd_payload.processed_sequence_number
        trigger_reason = cmd_payload.trigger_reason

        # 3. Deduplication Check
        async with self.session_factory() as session:
            repo = ProcessedEventRepository(session)
            if await repo.is_processed(event_id, consumer_id=self.CONSUMER_ID):
                self.duplicates_skipped += 1
                logger.info(
                    "[Agent Worker] Duplicate command event %s already processed. Skipping.",
                    event_id,
                )
                return True

        # 4. Check for Stale Trigger
        async with self.session_factory() as session:
            async with session.begin():
                exec_repo = AgentExecutionRepository(session)
                latest_seq = await exec_repo.get_latest_processed_sequence(session_id)
                if latest_seq > 0 and target_seq <= latest_seq:
                    self.executions_stale_skipped += 1
                    logger.info(
                        "[Agent Worker] Stale command for session %s: seq %d <= latest completed %d. Skipping.",
                        session_id,
                        target_seq,
                        latest_seq,
                    )
                    # Mark processed so we don't re-examine
                    proc_repo = ProcessedEventRepository(session)
                    await proc_repo.mark_processed(
                        event_id=event_id,
                        event_type=event.event_type,
                        session_id=session_id,
                        consumer_id=self.CONSUMER_ID,
                    )
                    return True

        # 5. Create Execution Record in PostgreSQL
        execution_id = None
        async with self.session_factory() as session:
            async with session.begin():
                exec_repo = AgentExecutionRepository(session)
                exec_model = await exec_repo.create_execution(
                    session_id=session_id,
                    trigger_reason=trigger_reason,
                    processed_sequence_number=target_seq,
                )
                execution_id = exec_model.execution_id

        self.executions_started += 1
        logger.info(
            "[Agent Worker] Started execution %s for session %s (seq=%d).",
            execution_id,
            session_id,
            target_seq,
        )

        # 6. Fetch Context from PostgreSQL
        async with self.session_factory() as session:
            session_repo = SessionRepository(session)
            conv_repo = ConversationRepository(session)
            sugg_repo = SuggestionRepository(session)
            session_model = await session_repo.get(session_id)
            if session_model is None:
                logger.error("[Agent Worker] Session %s not found in database.", session_id)
                await self._fail_execution(execution_id, "session_not_found")
                return True

            messages = await conv_repo.get_messages(session_id)
            prev_suggs = await sugg_repo.get_suggestions_for_session(session_id, limit=10)

        # 7. Execute Inference Pipeline (or Mock)
        suggestions_data: List[Dict[str, Any]] = []
        status_result = "no_suggestion"
        selected_model = None

        if self.mock_llm:
            # Budget Protection Mode: Automated testing without LLM API calls
            logger.info("[Agent Worker] MOCK_LLM enabled. Generating mock suggestion.")
            status_result = "suggestion_ready"
            selected_model = "mock-llm-budget-protection"
            suggestions_data = [
                {
                    "response_type": "perspective",
                    "suggestion_text": "Consider reviewing the asynchronous event pipeline latency metrics.",
                    "rationale": "Automated validation of end-to-end orchestration flow.",
                }
            ]
        else:
            try:
                # Construct state and invoke LangGraph
                initial_state = {
                    "session_id": str(session_id),
                    "situation": session_model.situation,
                    "objective": session_model.objective,
                    "communication_goals": session_model.communication_goals,
                    "response_preferences": (
                        session_model.response_preferences.model_dump()
                        if hasattr(session_model.response_preferences, "model_dump")
                        else dict(session_model.response_preferences)
                    ),
                    "constraints": session_model.constraints,
                    "participant_context": session_model.participant_context,
                    "messages": [
                        m.model_dump(mode="json")
                        for m in messages[-20:]
                    ],
                    "previous_suggestions": [
                        {
                            "suggestion_id": str(s.suggestion_id),
                            "response_type": s.response_type,
                            "suggestion_text": s.suggestion_text,
                            "text": s.suggestion_text,
                            "rationale": s.rationale,
                        }
                        for s in prev_suggs
                    ],
                    "suggestions": [],
                    "validation_warnings": [],
                }
                graph = get_agent_graph()
                result = await graph.ainvoke(initial_state)

                status_result = result.get("status", "no_suggestion")
                selected_model = result.get("selected_model", "deepseek/deepseek-v4.1-flash")
                raw_suggestions = result.get("suggestions", [])
                for item in raw_suggestions:
                    if isinstance(item, dict):
                        suggestions_data.append({
                            "response_type": item.get("response_type", "perspective"),
                            "suggestion_text": item.get("text") or item.get("suggestion_text", ""),
                            "rationale": item.get("rationale", ""),
                        })
            except StructuredOutputError as err:
                logger.error("[Agent Worker] Structured output error at stage %s: %s", err.stage, err)
                await self._fail_execution(execution_id, f"structured_output_{err.stage}")
                self.executions_failed += 1
                return True
            except Exception as err:
                logger.exception("[Agent Worker] Inference error during execution %s: %s", execution_id, err)
                await self._fail_execution(execution_id, "inference_exception")
                self.executions_failed += 1
                return True

        # 8. Persist Suggestions atomically with Outbox Events
        async with self.session_factory() as session:
            async with session.begin():
                sugg_repo = SuggestionRepository(session)
                exec_repo = AgentExecutionRepository(session)
                proc_repo = ProcessedEventRepository(session)

                if suggestions_data and status_result != "no_suggestion":
                    for item in suggestions_data:
                        text = item.get("suggestion_text") or item.get("text", "")
                        resp_type = item.get("response_type", "perspective")
                        rationale = item.get("rationale")
                        await sugg_repo.create_suggestion_atomic_with_outbox(
                            session_id=session_id,
                            execution_id=execution_id,
                            source_last_sequence_number=target_seq,
                            response_type=resp_type,
                            suggestion_text=text,
                            rationale=rationale,
                            selected_model=selected_model,
                        )
                        self.suggestions_generated += 1

                    await exec_repo.update_status(
                        execution_id=execution_id,
                        status="completed",
                        selected_model=selected_model,
                    )
                    self.executions_completed += 1
                    logger.info(
                        "[Agent Worker] Execution %s completed with %d suggestion(s).",
                        execution_id,
                        len(suggestions_data),
                    )
                else:
                    await exec_repo.update_status(
                        execution_id=execution_id,
                        status="no_suggestion",
                        selected_model=selected_model,
                    )
                    self.executions_no_suggestion += 1
                    logger.info(
                        "[Agent Worker] Execution %s completed with no suggestions (opportunity not found).",
                        execution_id,
                    )

                # Mark command processed
                await proc_repo.mark_processed(
                    event_id=event_id,
                    event_type=event.event_type,
                    session_id=session_id,
                    consumer_id=self.CONSUMER_ID,
                )

        return True

    async def _fail_execution(self, execution_id: Optional[UUID], failure_stage: str) -> None:
        """Mark an execution as failed in PostgreSQL."""
        if execution_id is None:
            return
        try:
            async with self.session_factory() as session:
                async with session.begin():
                    exec_repo = AgentExecutionRepository(session)
                    await exec_repo.update_status(
                        execution_id=execution_id,
                        status="failed",
                        failure_stage=failure_stage,
                    )
        except Exception as exc:
            logger.error("Failed to update execution %s to failed state: %s", execution_id, exc)
