"""Speech segment manager for incremental VAD, partials, and utterance finalization.

Maintains per-session audio streaming states, enforces speech/silence thresholds,
produces partial transcript updates, avoids duplicate finalized transcripts,
posts finalized messages to the Agent Server REST API, and dispatches updates
to the Delivery Service.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from uuid import UUID, uuid4

import httpx

from app.transcriber import TranscriptionResult, WhisperTranscriber
from app.vad import SileroVAD

logger = logging.getLogger(__name__)


@dataclass
class SessionSegmentState:
    """Per-session speech segmentation state."""

    session_id: str
    in_speech: bool = False
    speech_start_time: float = 0.0
    speech_duration_ms: float = 0.0
    silence_duration_ms: float = 0.0
    last_partial_time: float = 0.0
    last_partial_text: str = ""
    last_finalized_text: str = ""
    segment_sequence: int = 0
    activity_sequence: int = 0
    source_sequence: int = 0
    speech_onset_emitted: bool = False
    audio_remainder: bytearray = field(default_factory=bytearray)
    speech_buffer: bytearray = field(default_factory=bytearray)
    finalized_segments: List[str] = field(default_factory=list)


class SpeechSegmentManager:
    """Manages audio segmentation, VAD evaluation, and transcript dispatching."""

    def __init__(
        self,
        vad: Optional[SileroVAD] = None,
        transcriber: Optional[WhisperTranscriber] = None,
        agent_server_url: Optional[str] = None,
        delivery_service_url: Optional[str] = None,
        min_speech_duration_ms: float = 250.0,
        min_silence_duration_ms: float = 600.0,
        partial_interval_ms: float = 500.0,
        speech_onset_min_duration_ms: float = 64.0,
        speech_activity_publisher: Optional[Callable[[dict], Any]] = None,
    ) -> None:
        self.vad = vad or SileroVAD()
        self.transcriber = transcriber or WhisperTranscriber()
        self.agent_server_url = (
            agent_server_url
            or os.getenv("AGENT_SERVER_URL", "http://agent-api:8001").rstrip("/")
        )
        self.delivery_service_url = (
            delivery_service_url
            or os.getenv("DELIVERY_SERVICE_URL", "http://delivery-service:8003").rstrip("/")
        )
        self.min_speech_duration_ms = min_speech_duration_ms
        self.min_silence_duration_ms = min_silence_duration_ms
        self.partial_interval_ms = partial_interval_ms
        self.speech_onset_min_duration_ms = speech_onset_min_duration_ms
        self.speech_activity_publisher = speech_activity_publisher



        self.sessions: Dict[str, SessionSegmentState] = {}
        self._lock = asyncio.Lock()
        self._http_client: Optional[httpx.AsyncClient] = None

    async def get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    def get_or_create_session(self, session_id: str) -> SessionSegmentState:
        if session_id not in self.sessions:
            self.sessions[session_id] = SessionSegmentState(session_id=session_id)
            logger.info("Initialized speech segment state for session %s", session_id)
        return self.sessions[session_id]

    async def process_audio_chunk(
        self, session_id: str, chunk_pcm16: bytes
    ) -> List[TranscriptionResult]:
        """Process incoming raw audio chunk incrementally.

        Slices into 512-sample (1024-byte) VAD frames, updates speech/silence
        durations, triggers partial updates, and finalizes segments upon silence.
        Returns any finalized TranscriptionResults produced in this chunk.
        """
        state = self.get_or_create_session(session_id)
        finalized_results: List[TranscriptionResult] = []

        # Combine remainder from previous chunk
        state.audio_remainder.extend(chunk_pcm16)

        frame_size = SileroVAD.FRAME_SIZE_BYTES  # 1024 bytes (512 samples @ 16kHz)
        frame_duration_ms = (SileroVAD.FRAME_SIZE_SAMPLES / 16000.0) * 1000.0  # 32.0 ms

        while len(state.audio_remainder) >= frame_size:
            frame = bytes(state.audio_remainder[:frame_size])
            del state.audio_remainder[:frame_size]

            is_speech, prob, _ = self.vad.process_chunk(frame)

            if is_speech:
                # Speech detected
                if not state.in_speech:
                    state.in_speech = True
                    state.speech_start_time = time.time()
                    state.speech_duration_ms = 0.0
                    state.silence_duration_ms = 0.0
                    state.speech_buffer.clear()
                    state.speech_onset_emitted = False

                state.speech_buffer.extend(frame)
                state.speech_duration_ms += frame_duration_ms
                state.silence_duration_ms = 0.0

                # VAD Noise Protection: require minimum sustained speech duration before emitting onset
                if not state.speech_onset_emitted and state.speech_duration_ms >= self.speech_onset_min_duration_ms:
                    state.speech_onset_emitted = True
                    state.activity_sequence += 1
                    state.source_sequence += 1
                    await self._emit_speech_activity(state)

                # Check if we should emit a partial transcript
                now_ms = time.time() * 1000.0
                if (now_ms - state.last_partial_time) >= self.partial_interval_ms:
                    state.last_partial_time = now_ms
                    await self._emit_partial_transcript(state)


            else:
                # Silence detected
                if state.in_speech:
                    state.speech_buffer.extend(frame)  # include trailing silence padding
                    state.silence_duration_ms += frame_duration_ms

                    # Check if silence threshold exceeded to finalize segment
                    if state.silence_duration_ms >= self.min_silence_duration_ms:
                        final_res = await self._finalize_segment(state)
                        if final_res:
                            finalized_results.append(final_res)

        return finalized_results

    async def _emit_partial_transcript(self, state: SessionSegmentState) -> None:
        """Run fast partial transcription and send update to Delivery Service."""
        if len(state.speech_buffer) < 3200:  # < 100ms
            return

        result = self.transcriber.transcribe(
            bytes(state.speech_buffer), is_final=False
        )
        text = result.text.strip()
        if text and text != state.last_partial_text:
            state.last_partial_text = text
            logger.debug("Emitting partial transcript for %s: '%s'", state.session_id, text)
            await self._dispatch_to_delivery_service(
                state.session_id,
                {
                    "type": "partial",
                    "session_id": state.session_id,
                    "text": text,
                    "timestamp": time.time(),
                },
            )

    async def _finalize_segment(
        self, state: SessionSegmentState
    ) -> Optional[TranscriptionResult]:
        """Finalize speech utterance segment, persist to Agent Server, and notify Delivery Service."""
        speech_bytes = bytes(state.speech_buffer)
        duration_ms = state.speech_duration_ms

        # Reset speech state immediately
        state.in_speech = False
        state.speech_onset_emitted = False
        state.speech_buffer.clear()
        state.speech_duration_ms = 0.0
        state.silence_duration_ms = 0.0
        state.last_partial_text = ""

        # Discard clicks or noise shorter than threshold
        if duration_ms < self.min_speech_duration_ms or len(speech_bytes) < 3200:
            logger.debug("Discarding short utterance (%.1f ms) for %s", duration_ms, state.session_id)
            return None

        # Full transcription
        result = self.transcriber.transcribe(speech_bytes, is_final=True)
        text = result.text.strip()

        if not text:
            return None

        # Avoid duplicate finalized segments
        if text == state.last_finalized_text:
            logger.info("Prevented duplicate finalized segment for %s: '%s'", state.session_id, text)
            return None

        state.last_finalized_text = text
        state.segment_sequence += 1
        state.source_sequence += 1
        state.finalized_segments.append(text)

        logger.info(
            "Finalized transcript segment #%d for session %s: '%s' (lat=%.1fms)",
            state.segment_sequence,
            state.session_id,
            text,
            result.latency_ms,
        )

        # 1. Post to Agent Server API (persists in PostgreSQL & enqueues outbox event)
        agent_resp = await self._post_to_agent_server(state.session_id, text)

        # 2. Dispatch to Delivery Service for real-time client display
        delivery_payload = {
            "type": "final",
            "session_id": state.session_id,
            "text": text,
            "sequence_number": agent_resp.get("sequence_number", state.segment_sequence) if agent_resp else state.segment_sequence,
            "event_id": agent_resp.get("event_id") if agent_resp else None,
            "start_time": result.start_time,
            "end_time": result.end_time,
            "timestamp": time.time(),
        }
        await self._dispatch_to_delivery_service(state.session_id, delivery_payload)

        return result

    async def _post_to_agent_server(
        self, session_id: str, text: str
    ) -> Optional[dict]:
        """Post finalized transcript message to Agent Server API."""
        url = f"{self.agent_server_url}/v1/sessions/{session_id}/messages"
        payload = {"text": text, "source": "transcription"}
        try:
            client = await self.get_http_client()
            resp = await client.post(url, json=payload)
            if resp.status_code == 201:
                return resp.json()
            else:
                logger.warning(
                    "Agent Server responded with status %d for session %s: %s",
                    resp.status_code,
                    session_id,
                    resp.text,
                )
                return None
        except Exception as exc:
            logger.error(
                "Failed to post finalized message to Agent Server for session %s: %s",
                session_id,
                exc,
            )
            return None

    async def _emit_speech_activity(self, state: SessionSegmentState) -> None:
        """Emit real-time speech onset activity event to Agent Server / Kafka."""
        event_id = str(uuid4())
        now = time.time()
        payload = {
            "event_id": event_id,
            "session_id": state.session_id,
            "activity_type": "speech_onset",
            "activity_sequence": state.activity_sequence,
            "timestamp": now,
            "source_sequence": state.source_sequence,
            "last_finalized_sequence": state.segment_sequence,
        }

        if self.speech_activity_publisher:
            try:
                res = self.speech_activity_publisher(payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as exc:
                logger.debug("Error in custom speech_activity_publisher: %s", exc)
        else:
            await self._post_speech_activity_to_agent_server(state.session_id, payload)

    async def _post_speech_activity_to_agent_server(
        self, session_id: str, payload: dict
    ) -> None:
        """Post speech onset activity event to Agent Server API."""
        url = f"{self.agent_server_url}/v1/sessions/{session_id}/speech-activity"
        try:
            client = await self.get_http_client()
            resp = await client.post(url, json=payload, timeout=2.0)
            if resp.status_code not in (200, 201, 202):
                logger.debug("Non-2xx response from speech-activity: %s", resp.status_code)
        except Exception as exc:
            logger.debug("Failed to dispatch speech activity to agent server: %s", exc)

    async def _dispatch_to_delivery_service(
        self, session_id: str, payload: dict
    ) -> None:

        """Forward real-time partial or finalized event to Delivery Service."""
        url = f"{self.delivery_service_url}/api/delivery/{session_id}/events"
        try:
            client = await self.get_http_client()
            resp = await client.post(url, json=payload)
            if resp.status_code not in (200, 202):
                logger.debug("Delivery service returned %d for %s", resp.status_code, session_id)
        except Exception as exc:
            logger.debug(
                "Could not dispatch event to Delivery Service (%s): %s",
                url,
                exc,
            )

    async def flush_session(self, session_id: str) -> List[TranscriptionResult]:
        """Flush remaining buffered audio on disconnect."""
        state = self.sessions.get(session_id)
        if not state:
            return []

        results = []
        if state.in_speech and state.speech_duration_ms >= self.min_speech_duration_ms:
            res = await self._finalize_segment(state)
            if res:
                results.append(res)

        state.audio_remainder.clear()
        state.speech_buffer.clear()
        return results

    async def remove_session(self, session_id: str) -> None:
        """Remove session state and free resources."""
        await self.flush_session(session_id)
        async with self._lock:
            self.sessions.pop(session_id, None)
            logger.info("Cleaned up transcription session state for %s", session_id)
