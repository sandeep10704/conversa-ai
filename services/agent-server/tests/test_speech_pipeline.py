"""Comprehensive automated test suite for Audio Gateway, Transcription, Delivery, and Outbox Pipeline.

Covers all 15 required scenarios:
1. Audio connection establishment.
2. Session association.
3. Audio chunk forwarding.
4. VAD speech detection.
5. Silence detection.
6. Partial transcript generation.
7. Final transcript generation.
8. Per-session chronological ordering.
9. Duplicate final transcript prevention.
10. Posting finalized messages to Agent Server.
11. PostgreSQL persistence of finalized transcript messages.
12. Transactional outbox event generation.
13. Kafka publication.
14. Client disconnection and reconnection.
15. Multiple concurrent sessions.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import numpy as np
import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import importlib.util

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent


def _load_service_module(name: str, rel_path: str):
    path = str(BASE_DIR / rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Load audio-gateway modules
_mod_buf = _load_service_module("ag.buffer", "services/audio-gateway/app/buffer.py")
sys.modules["app.buffer"] = _mod_buf
_mod_cap = _load_service_module("ag.capture", "services/audio-gateway/app/capture.py")
SessionAudioBuffer = _mod_buf.SessionAudioBuffer
AudioGatewayManager = _mod_cap.AudioGatewayManager

# Load transcription-service modules
_mod_vad = _load_service_module("ts.vad", "services/transcription-service/app/vad.py")
sys.modules["app.vad"] = _mod_vad
_mod_trans = _load_service_module("ts.transcriber", "services/transcription-service/app/transcriber.py")
sys.modules["app.transcriber"] = _mod_trans
_mod_seg = _load_service_module("ts.segment_manager", "services/transcription-service/app/segment_manager.py")
SileroVAD = _mod_vad.SileroVAD
WhisperTranscriber = _mod_trans.WhisperTranscriber
TranscriptionResult = _mod_trans.TranscriptionResult
SpeechSegmentManager = _mod_seg.SpeechSegmentManager

# Load delivery-service modules
_mod_del_ws = _load_service_module("ds.websocket", "services/delivery-service/app/websocket.py")
DeliveryConnectionManager = _mod_del_ws.DeliveryConnectionManager

# Load agent-server modules
from app.database.models import (
    Base,
    ConversationMessageModel,
    OutboxEventModel,
    SessionModel,
)
from app.database.repository import (
    ConversationRepository,
    OutboxRepository,
    SessionRepository,
)
from app.publisher.service import OutboxPublisher
from app.schemas.conversation import TranscriptMessageRequest
from app.schemas.session import CreateSessionRequest


# In-memory test DB engine
@pytest.fixture
async def test_db_setup():
    test_engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    yield session_factory
    await test_engine.dispose()


class TestSpeechPipeline:
    """Automated unit and integration test suite covering the 15 requirements."""

    # 1. Audio connection establishment
    async def test_01_audio_connection_establishment(self):
        """Test audio gateway session registration and initial handshake."""
        manager = AudioGatewayManager(transcription_ws_url="ws://mock:8004/ws/transcribe")
        session_id = str(uuid4())
        mock_ws = MagicMock()

        state = await manager.register_session(session_id, mock_ws)
        assert state is not None
        assert state.session_id == session_id
        assert state.is_active is True
        assert session_id in manager.sessions

        await manager.disconnect_session(session_id)

    # 2. Session association
    async def test_02_session_association_and_validation(self):
        """Verify invalid UUIDs are rejected and valid sessions map strictly to session_id."""
        invalid_id = "not-a-valid-uuid"
        with pytest.raises(ValueError):
            UUID(invalid_id)

        valid_id = str(uuid4())
        assert UUID(valid_id) is not None

        manager = AudioGatewayManager()
        state = await manager.register_session(valid_id, MagicMock())
        assert state.session_id == valid_id
        assert manager.sessions[valid_id] is state

        await manager.disconnect_session(valid_id)

    # 3. Audio chunk forwarding
    async def test_03_audio_chunk_forwarding(self):
        """Verify PCM audio chunks are enqueued into session buffer and forwarded upstream."""
        manager = AudioGatewayManager()
        session_id = str(uuid4())
        state = await manager.register_session(session_id, MagicMock())

        # Mock upstream connection
        mock_upstream = AsyncMock()
        state.upstream_ws = mock_upstream
        state.upstream_connected = True

        chunk = b"\x00\x00" * 512  # 1024 bytes (512 samples)
        forwarded = await manager.forward_audio(session_id, chunk)

        assert forwarded is True
        assert state.chunks_received == 1
        assert state.chunks_forwarded == 1
        mock_upstream.send.assert_awaited_once_with(chunk)

        await manager.disconnect_session(session_id)

    # 4. VAD speech detection
    def test_04_vad_speech_detection(self):
        """Verify VAD detects high-energy/speech audio frames."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        # Generate high-amplitude 16-bit PCM speech frame
        samples = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16)
        speech_frame = samples.tobytes()

        is_speech, prob, lat_ms = vad.process_chunk(speech_frame)
        assert is_speech is True
        assert prob >= 0.5
        assert lat_ms >= 0.0

    # 5. Silence detection
    def test_05_vad_silence_detection(self):
        """Verify VAD correctly classifies silence frames."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        # Generate silence frame (all zeros)
        silence_frame = np.zeros(512, dtype=np.int16).tobytes()

        is_speech, prob, lat_ms = vad.process_chunk(silence_frame)
        assert is_speech is False
        assert prob < 0.5

    # 6. Partial transcript generation
    async def test_06_partial_transcript_generation(self):
        """Verify partial transcripts are generated during speech and sent to delivery service."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        transcriber.set_mock_text("I think we should")

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            partial_interval_ms=10.0,  # low interval for fast test
            min_speech_duration_ms=100.0,
            min_silence_duration_ms=500.0,
        )

        session_id = str(uuid4())
        dispatched_events = []

        async def mock_dispatch(sid, payload):
            dispatched_events.append(payload)

        manager._dispatch_to_delivery_service = mock_dispatch

        # Feed 15 speech frames (~480ms)
        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        for _ in range(15):
            await manager.process_audio_chunk(session_id, speech_frame)
            await asyncio.sleep(0.01)

        # Check that partial transcript was dispatched
        partials = [e for e in dispatched_events if e["type"] == "partial"]
        assert len(partials) >= 1
        assert partials[0]["text"] == "I think we should"
        assert partials[0]["session_id"] == session_id

    # 7. Final transcript generation
    async def test_07_final_transcript_generation(self):
        """Verify utterance finalization occurs when silence threshold is reached after speech."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        transcriber.set_mock_text("We should check database connection pool limits.")

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=150.0,
            min_silence_duration_ms=200.0,
        )

        session_id = str(uuid4())
        mock_post = AsyncMock(return_value={"sequence_number": 1, "event_id": str(uuid4())})
        manager._post_to_agent_server = mock_post
        manager._dispatch_to_delivery_service = AsyncMock()

        # 1. Send speech frames (10 frames = 320ms > 150ms)
        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        for _ in range(10):
            await manager.process_audio_chunk(session_id, speech_frame)

        # 2. Send silence frames (8 frames = 256ms > 200ms silence threshold)
        silence_frame = np.zeros(512, dtype=np.int16).tobytes()
        results = []
        for _ in range(8):
            res = await manager.process_audio_chunk(session_id, silence_frame)
            results.extend(res)

        assert len(results) == 1
        assert results[0].text == "We should check database connection pool limits."
        assert results[0].is_final is True
        mock_post.assert_awaited_once_with(session_id, "We should check database connection pool limits.")

    # 8. Per-session chronological ordering
    async def test_08_per_session_chronological_ordering(self):
        """Verify multiple utterances within a session maintain strict monotonic sequence numbers."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=100.0,
            min_silence_duration_ms=150.0,
        )

        session_id = str(uuid4())
        manager._post_to_agent_server = AsyncMock(return_value={"sequence_number": 1})
        manager._dispatch_to_delivery_service = AsyncMock()

        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        silence_frame = np.zeros(512, dtype=np.int16).tobytes()

        # Utterance 1
        transcriber.set_mock_text("First utterance")
        for _ in range(8):
            await manager.process_audio_chunk(session_id, speech_frame)
        for _ in range(6):
            await manager.process_audio_chunk(session_id, silence_frame)

        # Utterance 2
        transcriber.set_mock_text("Second utterance")
        for _ in range(8):
            await manager.process_audio_chunk(session_id, speech_frame)
        for _ in range(6):
            await manager.process_audio_chunk(session_id, silence_frame)

        state = manager.sessions[session_id]
        assert state.segment_sequence == 2
        assert state.finalized_segments == ["First utterance", "Second utterance"]

    # 9. Duplicate final transcript prevention
    async def test_09_duplicate_final_transcript_prevention(self):
        """Verify identical consecutive transcripts are filtered out to prevent duplicates."""
        vad = SileroVAD(threshold=0.5, mock_mode=True)
        transcriber = WhisperTranscriber(mock_mode=True)
        transcriber.set_mock_text("Same identical utterance")

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=100.0,
            min_silence_duration_ms=150.0,
        )

        session_id = str(uuid4())
        mock_post = AsyncMock(return_value={"sequence_number": 1})
        manager._post_to_agent_server = mock_post
        manager._dispatch_to_delivery_service = AsyncMock()

        speech_frame = (np.sin(np.linspace(0, 10, 512)) * 12000).astype(np.int16).tobytes()
        silence_frame = np.zeros(512, dtype=np.int16).tobytes()

        # First delivery
        for _ in range(8):
            await manager.process_audio_chunk(session_id, speech_frame)
        for _ in range(6):
            await manager.process_audio_chunk(session_id, silence_frame)

        # Second delivery (identical text)
        for _ in range(8):
            await manager.process_audio_chunk(session_id, speech_frame)
        for _ in range(6):
            await manager.process_audio_chunk(session_id, silence_frame)

        # Only one post should have occurred!
        assert mock_post.await_count == 1
        state = manager.sessions[session_id]
        assert len(state.finalized_segments) == 1

    # 10. Posting finalized messages to Agent Server
    async def test_10_posting_finalized_messages_to_agent_server(self):
        """Verify finalized message payload contract sent to Agent Server API."""
        manager = SpeechSegmentManager()
        session_id = str(uuid4())

        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {
            "event_id": str(uuid4()),
            "session_id": session_id,
            "sequence_number": 1,
            "text": "Production latency incident",
            "source": "transcription",
            "created_at": "2026-10-31T20:00:00Z",
        }
        mock_client.post.return_value = mock_resp
        manager.get_http_client = AsyncMock(return_value=mock_client)

        resp = await manager._post_to_agent_server(session_id, "Production latency incident")

        assert resp is not None
        assert resp["text"] == "Production latency incident"
        assert resp["source"] == "transcription"
        mock_client.post.assert_awaited_once_with(
            f"{manager.agent_server_url}/v1/sessions/{session_id}/messages",
            json={"text": "Production latency incident", "source": "transcription"},
        )

    # 11. PostgreSQL persistence of finalized transcript messages
    # 12. Transactional outbox event generation
    async def test_11_and_12_persistence_and_outbox_generation(self, test_db_setup):
        """Verify finalized transcript persists to DB and creates pending outbox record in same TX."""
        session_factory = test_db_setup
        async with session_factory() as session:
            session_repo = SessionRepository(session)
            conv_repo = ConversationRepository(session)
            outbox_repo = OutboxRepository(session)

            # Create session
            sess = await session_repo.create(
                CreateSessionRequest(situation="Speech Test", objective="Test persistence")
            )
            session_id = sess.session_id

            # Insert message from transcription
            msg_req = TranscriptMessageRequest(
                text="Latency on proxy service exceeded 500ms.",
                source="transcription",
            )
            msg_resp = await conv_repo.add_message(session_id, msg_req)

            # Verify DB persistence
            assert msg_resp.sequence_number == 1
            assert msg_resp.text == "Latency on proxy service exceeded 500ms."
            assert msg_resp.source == "transcription"

            # Verify Outbox record
            pending = await outbox_repo.fetch_pending_events(batch_size=10)
            assert len(pending) == 1
            outbox_item = pending[0]
            assert outbox_item.aggregate_id == session_id
            assert outbox_item.event_type == "conversation.transcript.final"
            assert outbox_item.payload["payload"]["text"] == "Latency on proxy service exceeded 500ms."
            assert outbox_item.payload["payload"]["source"] == "transcription"
            assert outbox_item.status == "pending"

    # 13. Kafka publication
    async def test_13_kafka_publication(self, test_db_setup):
        """Verify outbox publisher delivers the persisted transcript event to Kafka topic."""
        session_factory = test_db_setup
        async with session_factory() as session:
            async with session.begin():
                sess = await SessionRepository(session).create(
                    CreateSessionRequest(situation="Speech Test", objective="Test Kafka publish")
                )
                await ConversationRepository(session).add_message(
                    sess.session_id,
                    TranscriptMessageRequest(text="Check Kafka delivery", source="transcription"),
                )

        class MockProducer:
            def __init__(self):
                self.published_events = []
            async def publish(self, topic, key, value, event_id=None):
                self.published_events.append({"topic": topic, "key": key, "value": value, "event_id": event_id})
                mock_ack = MagicMock()
                mock_ack.topic = topic
                mock_ack.partition = 0
                mock_ack.offset = 1
                return mock_ack

        mock_producer = MockProducer()
        publisher = OutboxPublisher(session_factory=session_factory, producer=mock_producer)

        published_count = await publisher.publish_pending_batch()
        assert published_count == 1
        assert len(mock_producer.published_events) == 1
        published = mock_producer.published_events[0]
        assert published["topic"] == "conversation.transcript.final"
        assert published["key"] == str(sess.session_id)

        # Verify outbox event is marked published in database
        async with session_factory() as session:
            counts = await OutboxRepository(session).get_counts()
            assert counts["published"] == 1
            assert counts["pending"] == 0

    # 14. Client disconnection and reconnection
    async def test_14_client_disconnection_and_reconnection(self):
        """Verify clean resource disposal on disconnect and recovery on reconnection."""
        manager = AudioGatewayManager()
        session_id = str(uuid4())

        # 1. Connect first client
        mock_ws_1 = MagicMock()
        state1 = await manager.register_session(session_id, mock_ws_1)
        await state1.buffer.push(b"\x00" * 1000)
        assert manager.sessions[session_id].buffer.current_bytes == 1000

        # 2. Disconnect
        await manager.disconnect_session(session_id)
        assert session_id not in manager.sessions

        # 3. Reconnect with new connection
        mock_ws_2 = MagicMock()
        state2 = await manager.register_session(session_id, mock_ws_2)
        assert manager.sessions[session_id].buffer.current_bytes == 0
        assert state2.is_active is True

        await manager.disconnect_session(session_id)

    # 15. Multiple concurrent sessions
    async def test_15_multiple_concurrent_sessions_isolation(self):
        """Verify data from session A does not leak into session B across delivery and gateway."""
        delivery = DeliveryConnectionManager()
        sess_a = str(uuid4())
        sess_b = str(uuid4())

        ws_client_a = AsyncMock()
        ws_client_b = AsyncMock()

        await delivery.connect(sess_a, ws_client_a)
        await delivery.connect(sess_b, ws_client_b)

        # Broadcast to Session A
        await delivery.broadcast_to_session(sess_a, {"type": "final", "text": "Message for A"})

        # Verify Client A received message and Client B received NOTHING
        ws_client_a.send_text.assert_awaited_once()
        ws_client_b.send_text.assert_not_awaited()

        sent_payload_a = json.loads(ws_client_a.send_text.call_args[0][0])
        assert sent_payload_a["text"] == "Message for A"
        assert sent_payload_a["session_id"] == sess_a

        # Broadcast to Session B
        await delivery.broadcast_to_session(sess_b, {"type": "final", "text": "Message for B"})
        ws_client_b.send_text.assert_awaited_once()

        await delivery.disconnect(sess_a, ws_client_a)
        await delivery.disconnect(sess_b, ws_client_b)


class TestPrerecordedAudioIntegration:
    """Integration test using the generated prerecorded audio sample."""

    async def test_prerecorded_audio_end_to_end(self):
        """Feed prerecorded PCM utterance and verify segmentation, VAD, and transcription."""
        fixture_path = BASE_DIR / "tests" / "fixtures" / "audio" / "sample_utterance.pcm"
        assert fixture_path.exists(), "Audio fixture file must exist"

        with open(fixture_path, "rb") as f:
            audio_bytes = f.read()

        vad = SileroVAD(threshold=0.5, mock_mode=False)  # Real Silero VAD!
        transcriber = WhisperTranscriber(mock_mode=True)
        transcriber.set_mock_text("Prerecorded speech audio sample")

        manager = SpeechSegmentManager(
            vad=vad,
            transcriber=transcriber,
            min_speech_duration_ms=150.0,
            min_silence_duration_ms=300.0,
        )

        session_id = str(uuid4())
        manager._post_to_agent_server = AsyncMock(return_value={"sequence_number": 1})
        manager._dispatch_to_delivery_service = AsyncMock()

        # Feed audio in 100ms chunks (1600 samples = 3200 bytes)
        chunk_size = 3200
        final_results = []
        for i in range(0, len(audio_bytes), chunk_size):
            chunk = audio_bytes[i : i + chunk_size]
            results = await manager.process_audio_chunk(session_id, chunk)
            final_results.extend(results)

        # Flush any trailing audio
        flush_results = await manager.flush_session(session_id)
        final_results.extend(flush_results)

        assert len(final_results) >= 1
        assert final_results[0].text == "Prerecorded speech audio sample"
        manager._post_to_agent_server.assert_awaited()
