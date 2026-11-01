"""Conversa AI Transcription Service.

FastAPI entrypoint providing WebSocket endpoints for live speech audio streams,
VAD speech segmentation via Silero VAD, speech-to-text via faster-whisper,
and event dispatching to Agent Server and Delivery Service.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import UUID

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware

from app.segment_manager import SpeechSegmentManager
from app.transcriber import WhisperTranscriber
from app.vad import SileroVAD

vad = SileroVAD()
transcriber = WhisperTranscriber()
segment_manager = SpeechSegmentManager(vad=vad, transcriber=transcriber)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and graceful shutdown lifecycle."""
    yield
    await segment_manager.close()


app = FastAPI(
    title="Conversa AI - Transcription Service",
    version="0.1.0",
    description="Real-time VAD and ASR transcription service using Silero and faster-whisper",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health/live", tags=["Health"])
async def liveness_probe() -> dict:
    return {"service": "conversa-transcription-service", "status": "alive"}


@app.get("/health/ready", tags=["Health"])
async def readiness_probe() -> dict:
    return {
        "service": "conversa-transcription-service",
        "status": "ready",
        "vad_mock_mode": vad.mock_mode,
        "whisper_mock_mode": transcriber.mock_mode,
        "whisper_device": transcriber.device,
        "whisper_model_size": transcriber.model_size,
        "active_sessions": len(segment_manager.sessions),
    }


@app.get("/metrics", tags=["Observability"])
async def get_metrics() -> dict:
    total_segments = sum(
        len(s.finalized_segments) for s in segment_manager.sessions.values()
    )
    return {
        "active_sessions": len(segment_manager.sessions),
        "total_segments_finalized": total_segments,
        "vad_avg_latency_ms": round(vad.avg_latency_ms, 3),
        "transcription_avg_latency_ms": round(transcriber.avg_latency_ms, 3),
    }


@app.websocket("/ws/transcribe/{session_id}")
async def transcribe_websocket_endpoint(
    websocket: WebSocket, session_id: str
) -> None:
    """Internal WebSocket streaming endpoint receiving audio chunks from Audio Gateway."""
    try:
        UUID(session_id)
    except ValueError:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Invalid session_id UUID"
        )
        return

    await websocket.accept()

    try:
        while True:
            message = await websocket.receive()
            if "bytes" in message and message["bytes"]:
                chunk = message["bytes"]
                if chunk == b"__EOS__":
                    await segment_manager.flush_session(session_id)
                    break
                await segment_manager.process_audio_chunk(session_id, chunk)

            elif "text" in message and message["text"]:
                if message["text"] == "__EOS__":
                    await segment_manager.flush_session(session_id)
                    break

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        pass
    finally:
        await segment_manager.remove_session(session_id)
