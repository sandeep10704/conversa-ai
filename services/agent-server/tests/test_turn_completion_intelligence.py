"""Comprehensive automated test suite for Conversational Turn-Completion Intelligence (Gate 1).

Covers all required Phase 1 scenarios:
1. Syntactic turn-completion classification (question, declarative, incomplete).
2. Custom configurable timing thresholds.
3. VAD speech-onset detection (emitted only on transitions, not every frame).
4. Monotonic activity sequence numbering and payload contract.
5. Immediate turn-completion timer cancellation upon speech resumption.
6. Clean turn completion and command dispatch when pause is undisturbed.
7. Deduplication of identical speech activity events.
8. Rejection of stale out-of-order speech activity events.
9. Out-of-order topic consumption (speech.activity before transcript.final).
10. Realistic multi-turn replay integration test with mid-thought pause vs true turn completion.

All tests run completely offline with mocked dependencies and zero OpenRouter requests.
"""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
import importlib.util

import numpy as np
import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent


def _load_service_module(name: str, rel_path: str):
    path = str(BASE_DIR / rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Load transcription-service modules dynamically
_mod_vad = _load_service_module("ts_tc.vad", "services/transcription-service/app/vad.py")
sys.modules["app.vad"] = _mod_vad
_mod_trans = _load_service_module("ts_tc.transcriber", "services/transcription-service/app/transcriber.py")
sys.modules["app.transcriber"] = _mod_trans
_mod_seg = _load_service_module("ts_tc.segment_manager", "services/transcription-service/app/segment_manager.py")
SileroVAD = _mod_vad.SileroVAD
WhisperTranscriber = _mod_trans.WhisperTranscriber
SpeechSegmentManager = _mod_seg.SpeechSegmentManager


from app.core.config import Settings
from app.database.models import (
    AgentExecutionModel,
    Base,
    ConversationMessageModel,
    ProcessedEventModel,
    SessionModel,
)
from app.events.commands import AgentCommandEvent
from app.events.speech_activity import SpeechActivityEvent, SpeechActivityPayload
from app.events.topics import KafkaTopics
from app.events.transcript import TranscriptFinalEvent, TranscriptFinalPayload
from app.orchestration.trigger_controller import (
    AgentTriggerController,
    SessionTriggerState,
    TurnState,
)

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
        self.value = value if isinstance(value, bytes) else value.encode("utf-8")


class MockEventProducer:
    """In-memory mock event producer capturing published events."""

    def __init__(self) -> None:
        self.published_events: List[Dict[str, Any]] = []
        self.is_started = False

    async def start(self) -> None:
        self.is_started = True

    async def stop(self) -> None:
        self.is_started = False

    async def publish(
        self, topic: str, key: str, value: Dict[str, Any], event_id: Optional[str] = None
    ) -> None:
        self.published_events.append(
            {"topic": topic, "key": key, "value": value, "event_id": event_id}
        )


@pytest.fixture
async def db_session_factory():
    """Create an isolated, in-memory SQLite database for trigger controller tests."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    yield session_factory
    await engine.dispose()


def make_transcript_record(
    session_id: UUID,
    seq: int,
    text: str,
    event_id: Optional[UUID] = None,
    offset: int = 0,
    source_sequence: Optional[int] = None,
    associated_activity_sequence: Optional[int] = None,
    occurred_at: Optional[datetime] = None,
) -> MockKafkaRecord:
    """Build a mock Kafka record for conversation.transcript.final."""
    ev_id = event_id or uuid4()
    occ = occurred_at or datetime.now(timezone.utc)
    payload = {
        "event_id": str(ev_id),
        "event_type": KafkaTopics.TRANSCRIPT_FINAL.value,
        "event_version": "v1",
        "session_id": str(session_id),
        "sequence_number": seq,
        "occurred_at": occ.isoformat(),
        "payload": {
            "sequence_number": seq,
            "text": text,
            "source": "transcription",
            "source_sequence": source_sequence,
            "associated_activity_sequence": associated_activity_sequence,
        },
    }
    return MockKafkaRecord(
        value=json.dumps(payload),
        topic=KafkaTopics.TRANSCRIPT_FINAL.value,
        offset=offset,
        key=str(session_id).encode("utf-8"),
    )


def make_speech_activity_record(
    session_id: UUID,
    seq: int,
    activity_type: str = "speech_onset",
    timestamp: Optional[float] = None,
    event_id: Optional[UUID] = None,
    offset: int = 0,
    source_sequence: Optional[int] = None,
    last_finalized_sequence: Optional[int] = None,
) -> MockKafkaRecord:
    """Build a mock Kafka record for conversation.speech.activity."""
    ev_id = event_id or uuid4()
    ts = timestamp if timestamp is not None else time.time()
    payload = {
        "event_id": str(ev_id),
        "event_type": KafkaTopics.SPEECH_ACTIVITY.value,
        "event_version": "v1",
        "session_id": str(session_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "activity_type": activity_type,
            "activity_sequence": seq,
            "timestamp": ts,
            "source_sequence": source_sequence,
            "last_finalized_sequence": last_finalized_sequence,
        },
    }
    return MockKafkaRecord(
        value=json.dumps(payload),
        topic=KafkaTopics.SPEECH_ACTIVITY.value,
        offset=offset,
        key=str(session_id).encode("utf-8"),
    )


class TestTurnCompletionClassification:
    """Tests for syntactic and lexical turn completion classification."""

    def test_01_question_classification_terminal_question_mark(self):
        controller = AgentTriggerController(
            turn_completion_enabled=True,
            turn_pause_question_ms=1500,
            turn_pause_complete_ms=2500,
            turn_pause_incomplete_ms=4500,
        )
        assert controller.classify_turn_pause("What is our target latency?") == 1500
        assert controller.classify_turn_pause("Are you sure about this?") == 1500

    def test_02_question_classification_interrogative_starter(self):
        controller = AgentTriggerController(
            turn_completion_enabled=True,
            turn_pause_question_ms=1500,
            turn_pause_complete_ms=2500,
            turn_pause_incomplete_ms=4500,
        )
        # Missing punctuation but starts with interrogative phrasing
        assert controller.classify_turn_pause("what do you think about postgresql") == 1500
        assert controller.classify_turn_pause("can we verify the kafka throughput") == 1500

    def test_03_incomplete_classification_trailing_punctuation(self):
        controller = AgentTriggerController(
            turn_completion_enabled=True,
            turn_pause_question_ms=1500,
            turn_pause_complete_ms=2500,
            turn_pause_incomplete_ms=4500,
        )
        assert controller.classify_turn_pause("We should check the latency,") == 4500
        assert controller.classify_turn_pause("I was thinking that maybe...") == 4500

    def test_04_incomplete_classification_trailing_conjunction(self):
        controller = AgentTriggerController(
            turn_completion_enabled=True,
            turn_pause_question_ms=1500,
            turn_pause_complete_ms=2500,
            turn_pause_incomplete_ms=4500,
        )
        assert controller.classify_turn_pause("We could optimize the database query and") == 4500
        assert controller.classify_turn_pause("The results look promising but") == 4500
        assert controller.classify_turn_pause("We have high memory usage because") == 4500

    def test_05_declarative_classification_complete_sentence(self):
        controller = AgentTriggerController(
            turn_completion_enabled=True,
            turn_pause_question_ms=1500,
            turn_pause_complete_ms=2500,
            turn_pause_incomplete_ms=4500,
        )
        assert controller.classify_turn_pause("The audio pipeline throughput is 50 requests per second.") == 2500
        assert controller.classify_turn_pause("All migrations passed successfully") == 2500

    def test_06_custom_configurable_timings(self):
        controller = AgentTriggerController(
            turn_completion_enabled=True,
            turn_pause_question_ms=300,
            turn_pause_complete_ms=600,
            turn_pause_incomplete_ms=1200,
        )
        assert controller.classify_turn_pause("Ready?") == 300
        assert controller.classify_turn_pause("Done.") == 600
        assert controller.classify_turn_pause("Wait,") == 1200


class TestSpeechOnsetDetection:
    """Tests for VAD speech onset transition detection and sequence ordering."""

    async def test_07_speech_onset_published_only_on_transition(self):
        """Speech onset events must fire exactly once on silence-to-speech transition, not every frame."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        emitted_activities = []

        def capture_activity(payload):
            emitted_activities.append(payload)

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=100.0,
            min_silence_duration_ms=400.0,
            speech_activity_publisher=capture_activity,
        )

        session_id = str(uuid4())
        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()

        # Send 10 speech frames continuously (one speech burst)
        for _ in range(10):
            await manager.process_audio_chunk(session_id, speech_frame)

        # Assert: Exactly ONE speech onset event emitted for the whole burst
        assert len(emitted_activities) == 1
        assert emitted_activities[0]["activity_type"] == "speech_onset"
        assert emitted_activities[0]["activity_sequence"] == 1
        assert emitted_activities[0]["session_id"] == session_id

    async def test_08_multiple_speech_bursts_increment_sequence(self):
        """Consecutive speech bursts separated by silence increment activity_sequence monotonically."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        transcriber.set_mock_text("Test utterance")
        emitted_activities = []

        def capture_activity(payload):
            emitted_activities.append(payload)

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=100.0,
            min_silence_duration_ms=200.0,
            speech_activity_publisher=capture_activity,
        )

        session_id = str(uuid4())
        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        silent_frame = np.zeros(512, dtype=np.int16).tobytes()

        # Burst 1
        for _ in range(5):
            await manager.process_audio_chunk(session_id, speech_frame)

        # Silence to finalize burst 1 (min_silence_duration_ms = 200ms -> ~7 frames of 32ms)
        for _ in range(10):
            await manager.process_audio_chunk(session_id, silent_frame)

        # Burst 2
        for _ in range(5):
            await manager.process_audio_chunk(session_id, speech_frame)

        # Assert: Exactly TWO speech onset events emitted with sequences 1 and 2
        assert len(emitted_activities) == 2
        assert emitted_activities[0]["activity_sequence"] == 1
        assert emitted_activities[1]["activity_sequence"] == 2

    async def test_08b_vad_transient_noise_below_64ms_does_not_emit_speech_onset(self):
        """Transient acoustic noise (<64ms / 1 Silero frame) must not emit speech onset events."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        emitted_activities = []

        def capture_activity(payload):
            emitted_activities.append(payload)

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=250.0,
            min_silence_duration_ms=200.0,
            speech_onset_min_duration_ms=64.0,  # 2 frames threshold
            speech_activity_publisher=capture_activity,
        )

        session_id = str(uuid4())
        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        silent_frame = np.zeros(512, dtype=np.int16).tobytes()

        # Send exactly 1 speech frame (32ms < 64ms threshold) - simulates a mic click or cough
        await manager.process_audio_chunk(session_id, speech_frame)

        # Then send silence (10 frames = 320ms > 200ms min_silence)
        for _ in range(10):
            await manager.process_audio_chunk(session_id, silent_frame)

        # Assert: Noise transient was filtered out; zero speech activity events emitted!
        assert len(emitted_activities) == 0

    async def test_08c_vad_sustained_speech_64ms_emits_speech_onset(self):
        """Sustained speech (>=64ms / 2 Silero frames) must emit speech onset event."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        emitted_activities = []

        def capture_activity(payload):
            emitted_activities.append(payload)

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=250.0,
            min_silence_duration_ms=200.0,
            speech_onset_min_duration_ms=64.0,
            speech_activity_publisher=capture_activity,
        )

        session_id = str(uuid4())
        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()

        # Send exactly 2 speech frames (64ms >= 64ms threshold)
        for _ in range(2):
            await manager.process_audio_chunk(session_id, speech_frame)

        # Assert: Speech onset event is emitted on the 2nd frame!
        assert len(emitted_activities) == 1
        assert emitted_activities[0]["activity_type"] == "speech_onset"
        assert emitted_activities[0]["activity_sequence"] == 1

    async def test_08d_transient_noise_does_not_cancel_pending_turn_timer(self, db_session_factory):
        """Transient noise (<64ms) does not emit onset and therefore does not cancel a pending turn timer."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=80,
            turn_pause_complete_ms=100,
            turn_pause_incomplete_ms=200,
        )

        session_id = uuid4()
        # Utterance 1 completes and starts 80ms question timer
        rec1 = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="Can we verify the deployment status?",
            offset=1,
        )
        await controller.process_record(rec1)

        state = controller.get_or_create_session_state(session_id)
        assert state.turn_state == TurnState.UTTERANCE_PAUSED
        assert state.turn_timer_task is not None

        # Audio segment manager processes a 32ms transient noise click (<64ms)
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        emitted_events = []
        seg_manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            speech_onset_min_duration_ms=64.0,
            speech_activity_publisher=lambda p: emitted_events.append(p),
        )
        click_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        await seg_manager.process_audio_chunk(str(session_id), click_frame)

        # Verify no speech activity was emitted by segment manager
        assert len(emitted_events) == 0

        # Assert: Trigger controller timer is still running intact
        assert state.turn_timer_task is not None
        assert controller.turn_timers_cancelled == 0

        # Wait for undisturbed turn pause (80ms + margin)
        await asyncio.sleep(0.12)

        # Assert: Turn timer completed and trigger was emitted
        assert controller.turns_completed == 1
        assert controller.triggers_emitted == 1


class TestTurnCompletionStateMachine:
    """Tests for the Trigger Controller turn-completion state machine, cancellations, and ordering."""

    async def test_09_turn_timer_cancellation_on_speech_resumption(self, db_session_factory):
        """Pending turn-completion timer is immediately aborted when speech resumes."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=150,
            turn_pause_complete_ms=200,
            turn_pause_incomplete_ms=300,
        )

        session_id = uuid4()
        # Utterance 1 completes
        rec_final = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="What is our database connection pool size?",
            offset=1,
        )
        await controller.process_record(rec_final)

        # State should be UTTERANCE_PAUSED with active turn timer task
        state = controller.get_or_create_session_state(session_id)
        assert state.turn_state == TurnState.UTTERANCE_PAUSED
        assert state.turn_timer_task is not None
        assert not state.turn_timer_task.done()

        # Speaker resumes speaking after 30ms (before 150ms timer expires)
        await asyncio.sleep(0.03)
        rec_speech = make_speech_activity_record(
            session_id=session_id,
            seq=1,
            activity_type="speech_onset",
            offset=2,
        )
        await controller.process_record(rec_speech)

        # Assert: Turn timer cancelled immediately!
        assert state.turn_state == TurnState.SPEAKING
        assert state.turn_timer_task is None
        assert controller.turn_timers_cancelled == 1

        # Wait past original 150ms timer duration to confirm no trigger occurred
        await asyncio.sleep(0.2)
        assert len(producer.published_events) == 0
        assert controller.triggers_emitted == 0

    async def test_10_turn_timer_fires_when_pause_undisturbed(self, db_session_factory):
        """When conversational turn pause elapses without speech resumption, agent trigger is emitted."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=50,
            turn_pause_complete_ms=50,
            turn_pause_incomplete_ms=100,
        )

        session_id = uuid4()
        rec_final = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="The API latency is well within our 200ms budget.",
            offset=1,
        )
        await controller.process_record(rec_final)

        # Wait for the 50ms pause to elapse undisturbed
        await asyncio.sleep(0.1)

        state = controller.get_or_create_session_state(session_id)
        assert state.turn_state == TurnState.TURN_COMPLETED
        assert controller.turns_completed == 1
        assert controller.triggers_emitted == 1
        assert len(producer.published_events) == 1
        published = producer.published_events[0]
        assert published["topic"] == KafkaTopics.AGENT_COMMANDS.value
        assert published["value"]["payload"]["trigger_reason"] == "turn_completed"

    async def test_11_duplicate_speech_activity_event_rejected(self, db_session_factory):
        """Duplicate speech activity events with equal sequence are discarded."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            turn_completion_enabled=True,
        )

        session_id = uuid4()
        rec1 = make_speech_activity_record(session_id=session_id, seq=1, offset=1)
        rec2_duplicate = make_speech_activity_record(session_id=session_id, seq=1, offset=2)

        await controller.process_record(rec1)
        assert controller.stale_speech_events_skipped == 0

        # Deliver duplicate seq=1
        await controller.process_record(rec2_duplicate)
        assert controller.stale_speech_events_skipped == 1

    async def test_12_stale_out_of_order_speech_activity_event_discarded(self, db_session_factory):
        """Out-of-order speech activity event with an older sequence or timestamp is discarded."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            turn_completion_enabled=True,
        )

        session_id = uuid4()
        # Event with sequence 2 at t=10.0
        rec_seq2 = make_speech_activity_record(
            session_id=session_id, seq=2, timestamp=10.0, offset=1
        )
        # Stale event with sequence 1 at t=5.0
        rec_seq1 = make_speech_activity_record(
            session_id=session_id, seq=1, timestamp=5.0, offset=2
        )

        await controller.process_record(rec_seq2)
        state = controller.get_or_create_session_state(session_id)
        assert state.last_activity_sequence == 2

        # Process stale seq=1
        await controller.process_record(rec_seq1)
        assert controller.stale_speech_events_skipped == 1
        assert state.last_activity_sequence == 2

    async def test_13_speech_activity_arrives_before_transcript_final(self, db_session_factory):
        """Handles non-assumed ordering: speech onset arrives before transcript.final."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=60,
        )

        session_id = uuid4()
        # Onset arrives first (speaker started talking)
        rec_onset = make_speech_activity_record(session_id=session_id, seq=1, offset=1)
        await controller.process_record(rec_onset)
        state = controller.get_or_create_session_state(session_id)
        assert state.turn_state == TurnState.SPEAKING

        # Later, transcript.final arrives after utterance finalization
        rec_final = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="Can we deploy the update tonight?",
            offset=2,
        )
        await controller.process_record(rec_final)
        assert state.turn_state == TurnState.UTTERANCE_PAUSED

        # Turn pause elapses undisturbed
        await asyncio.sleep(0.1)
        assert controller.triggers_emitted == 1


class TestTurnCompletionReplayIntegration:
    """End-to-end replay integration simulating multi-turn speech stream."""

    async def test_14_replay_mid_thought_pause_prevented_and_turn_completion_triggered(
        self, db_session_factory
    ):
        """Simulate real meeting scenario:
        1. Speaker says: 'We should verify the cache TTL,' (incomplete thought, ends with comma).
        2. Speaker pauses briefly (30ms).
        3. Speaker resumes: 'especially before running the load test.' (complete thought).
        4. Turn timer for utterance 1 is cancelled before firing.
        5. Utterance 2 completes, speaker genuinely stops talking, and turn timer triggers agent!
        """
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=50,
            turn_pause_complete_ms=80,
            turn_pause_incomplete_ms=200,
        )


        session_id = uuid4()

        # Phase A: Utterance 1 spoken and finalized
        rec1_onset = make_speech_activity_record(session_id=session_id, seq=1, offset=1)
        await controller.process_record(rec1_onset)

        rec1_final = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="We should verify the cache TTL,",  # Classified as incomplete -> 200ms pause
            offset=2,
        )
        await controller.process_record(rec1_final)

        state = controller.get_or_create_session_state(session_id)
        assert state.turn_state == TurnState.UTTERANCE_PAUSED
        assert state.turn_timer_task is not None

        # Phase B: Speaker resumes speaking after 30ms (mid-thought pause)
        await asyncio.sleep(0.03)
        rec2_onset = make_speech_activity_record(session_id=session_id, seq=2, offset=3)
        await controller.process_record(rec2_onset)

        # Assert: Timer cancelled immediately
        assert controller.turn_timers_cancelled == 1
        assert state.turn_state == TurnState.SPEAKING
        assert len(producer.published_events) == 0

        # Phase C: Utterance 2 finalized
        rec2_final = make_transcript_record(
            session_id=session_id,
            seq=2,
            text="especially before running the load test.",  # Complete thought -> 80ms pause
            offset=4,
        )
        await controller.process_record(rec2_final)
        assert state.turn_state == TurnState.UTTERANCE_PAUSED

        # Phase D: Genuine turn handoff (speaker stops talking for 100ms > 80ms pause)
        await asyncio.sleep(0.12)

        # Assert: Exactly ONE trigger emitted for the entire turn!
        assert controller.turns_completed == 1
        assert controller.triggers_emitted == 1
        assert len(producer.published_events) == 1
        cmd = producer.published_events[0]["value"]["payload"]
        assert cmd["processed_sequence_number"] == 2
        assert cmd["trigger_reason"] == "turn_completed"


class TestMinimumMessageCountAndQuestionSensitivity:
    """Tests confirming single completed questions trigger turn completion while incomplete/declarative require min_messages."""

    async def test_15_single_actionable_question_triggers_with_min_messages_3(self, db_session_factory):
        """A single completed actionable question triggers turn completion even when min_new_messages=3."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=3,  # Production default
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=60,
            turn_pause_complete_ms=100,
            turn_pause_incomplete_ms=200,
        )

        session_id = uuid4()
        # Single message: clear actionable question
        rec = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="Can we deploy the update tonight?",
            offset=1,
        )
        await controller.process_record(rec)

        state = controller.get_or_create_session_state(session_id)
        # Verify turn timer was scheduled despite new_messages_count (1) < min_new_messages (3)
        assert state.new_messages_count == 1
        assert state.turn_timer_task is not None

        # Allow turn pause (60ms) to elapse
        await asyncio.sleep(0.09)

        assert controller.turns_completed == 1
        assert controller.triggers_emitted == 1
        assert len(producer.published_events) == 1
        cmd = producer.published_events[0]["value"]["payload"]
        assert cmd["processed_sequence_number"] == 1
        assert cmd["trigger_reason"] == "turn_completed"

    async def test_16_single_incomplete_thought_does_not_trigger_with_min_messages_3(self, db_session_factory):
        """A single incomplete thought does not trigger turn completion when min_new_messages=3."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=3,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_incomplete_ms=60,
        )

        session_id = uuid4()
        # Single message: incomplete thought ending with comma
        rec = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="We should check the cache TTL,",
            offset=1,
        )
        await controller.process_record(rec)

        state = controller.get_or_create_session_state(session_id)
        assert state.new_messages_count == 1
        # No timer scheduled because incomplete thought requires min_new_messages=3
        assert state.turn_timer_task is None

        await asyncio.sleep(0.09)
        assert controller.triggers_emitted == 0
        assert len(producer.published_events) == 0

    async def test_17_single_declarative_statement_does_not_trigger_with_min_messages_3(self, db_session_factory):
        """A single declarative statement does not trigger turn completion when min_new_messages=3."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=3,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_complete_ms=60,
        )

        session_id = uuid4()
        # Single message: complete declarative sentence
        rec = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="The deployment is scheduled for 10 PM.",
            offset=1,
        )
        await controller.process_record(rec)

        state = controller.get_or_create_session_state(session_id)
        assert state.new_messages_count == 1
        assert state.turn_timer_task is None

        await asyncio.sleep(0.09)
        assert controller.triggers_emitted == 0
        assert len(producer.published_events) == 0

    async def test_18_three_declarative_messages_trigger_with_min_messages_3(self, db_session_factory):
        """Three declarative messages satisfy min_new_messages=3 and trigger turn completion."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=3,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_complete_ms=60,
        )

        session_id = uuid4()
        # Message 1
        await controller.process_record(
            make_transcript_record(session_id, 1, "First observation about the system.", offset=1)
        )
        state = controller.get_or_create_session_state(session_id)
        assert state.turn_timer_task is None

        # Message 2
        await controller.process_record(
            make_transcript_record(session_id, 2, "Second observation about the latency.", offset=2)
        )
        assert state.turn_timer_task is None

        # Message 3
        await controller.process_record(
            make_transcript_record(session_id, 3, "Third observation concludes the overview.", offset=3)
        )
        assert state.turn_timer_task is not None

        await asyncio.sleep(0.09)
        assert controller.turns_completed == 1
        assert controller.triggers_emitted == 1
        assert len(producer.published_events) == 1
        cmd = producer.published_events[0]["value"]["payload"]
        assert cmd["processed_sequence_number"] == 3


class TestCrossTopicKafkaOrdering:
    """Tests verifying cross-topic Kafka ordering anomalies, out-of-order delivery, and stale timer prevention."""

    async def test_19_cross_topic_race_speech_activity_before_delayed_transcript_final(self, db_session_factory):
        """Case A: Speech activity for utterance 2 arrives BEFORE delayed transcript.final for utterance 1.

        The controller remains in SPEAKING state and does NOT schedule a turn timer for utterance 1.
        """
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=60,
            turn_pause_complete_ms=80,
        )

        session_id = uuid4()

        # Step 1: Speaker resumed speaking Utterance 2 at t=10.0, so speech.activity arrives first.
        # last_finalized_sequence=1 indicates utterance 1 was finalized before this onset.
        rec_onset2 = make_speech_activity_record(
            session_id=session_id,
            seq=2,
            timestamp=10.0,
            source_sequence=3,
            last_finalized_sequence=1,
            offset=1,
        )
        await controller.process_record(rec_onset2)

        state = controller.get_or_create_session_state(session_id)
        assert state.turn_state == TurnState.SPEAKING

        # Step 2: Now the delayed transcript.final for Utterance 1 arrives
        rec_final1 = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="Can we verify the database migration?",
            source_sequence=2,
            associated_activity_sequence=1,
            offset=2,
        )
        await controller.process_record(rec_final1)

        # Assert: Trigger controller detects participant is already speaking Utterance 2!
        # State MUST remain SPEAKING, and NO timer should be started for the old utterance.
        assert state.turn_state == TurnState.SPEAKING
        assert state.turn_timer_task is None

        # Wait longer than pause duration: no trigger should fire
        await asyncio.sleep(0.1)
        assert controller.triggers_emitted == 0
        assert len(producer.published_events) == 0

    async def test_20_cross_topic_race_speech_resumption_cancels_timer_at_95_percent_expiry(self, db_session_factory):
        """Case B: Speech resumption arrives just before timer expiry (95% of pause threshold).

        The pending turn timer is cancelled cleanly, preventing stale dispatch.
        """
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=100,  # 100ms pause
        )

        session_id = uuid4()
        # Utterance 1 completes -> starts 100ms timer
        rec1_final = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="Should we check the replica lag?",
            offset=1,
        )
        await controller.process_record(rec1_final)

        state = controller.get_or_create_session_state(session_id)
        assert state.turn_timer_task is not None

        # Wait for 95ms (95% of 100ms threshold)
        await asyncio.sleep(0.095)

        # Participant resumes speaking right at the boundary!
        rec2_onset = make_speech_activity_record(
            session_id=session_id,
            seq=2,
            timestamp=time.time(),
            offset=2,
        )
        await controller.process_record(rec2_onset)

        # Assert: Turn timer cancelled immediately
        assert controller.turn_timers_cancelled == 1
        assert state.turn_state == TurnState.SPEAKING
        assert state.turn_timer_task is None

        # Wait another 40ms (well past original 100ms deadline)
        await asyncio.sleep(0.04)

        # Assert: No trigger was emitted!
        assert controller.triggers_emitted == 0
        assert len(producer.published_events) == 0

    async def test_21_stale_timer_task_ignored_if_speech_resumes_right_at_expiry(self, db_session_factory):
        """Verify lock-protected stale timer guard: if speech resumes while timer is waking, dispatch is aborted."""
        producer = MockEventProducer()
        controller = AgentTriggerController(
            session_factory=db_session_factory,
            producer=producer,
            auto_run=True,
            min_new_messages=1,
            cooldown_seconds=0.0,
            turn_completion_enabled=True,
            turn_pause_question_ms=40,
        )

        session_id = uuid4()
        rec_final = make_transcript_record(
            session_id=session_id,
            seq=1,
            text="What is the current CPU utilization?",
            offset=1,
        )
        await controller.process_record(rec_final)

        state = controller.get_or_create_session_state(session_id)
        assert state.turn_timer_task is not None

        # Directly set state to SPEAKING simulating a concurrent speech onset event setting state under lock
        async with controller._lock:
            state.turn_state = TurnState.SPEAKING
            # Supersede timer task reference
            state.turn_timer_task = None

        # Wait past timer pause
        await asyncio.sleep(0.06)

        # Assert: Stale timer aborted cleanly without dispatching command
        assert controller.triggers_emitted == 0
        assert len(producer.published_events) == 0
