"""Audio session connection manager and transcription upstream forwarding.

Manages client WebSocket sessions, per-session audio buffers, and real-time
forwarding over internal WebSockets to the downstream Transcription Service.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

import websockets
from fastapi import WebSocket

from app.buffer import SessionAudioBuffer

logger = logging.getLogger(__name__)


@dataclass
class SessionState:
    """Encapsulates active session resources and statistics."""

    session_id: str
    client_ws: WebSocket
    buffer: SessionAudioBuffer
    upstream_ws: Any = None
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    chunks_received: int = 0
    chunks_forwarded: int = 0
    total_forward_latency_ms: float = 0.0
    is_active: bool = True
    upstream_connected: bool = False


class AudioGatewayManager:
    """Manages concurrent client sessions and forwards audio to Transcription Service."""

    def __init__(self, transcription_ws_url: Optional[str] = None) -> None:
        self.transcription_ws_url = (
            transcription_ws_url
            or os.getenv(
                "TRANSCRIPTION_SERVICE_WS_URL",
                "ws://transcription-service:8004/ws/transcribe",
            )
        )
        self.sessions: Dict[str, SessionState] = {}
        self._lock = asyncio.Lock()

    async def register_session(
        self, session_id: str, client_ws: WebSocket
    ) -> SessionState:
        """Register a new client session and allocate buffer."""
        async with self._lock:
            # If an existing session with same ID exists, clean it up first
            if session_id in self.sessions:
                logger.info("Cleaning up existing session %s before registering new one", session_id)
                await self._cleanup_session(self.sessions[session_id])

            buffer = SessionAudioBuffer(session_id=session_id)
            state = SessionState(
                session_id=session_id,
                client_ws=client_ws,
                buffer=buffer,
            )
            self.sessions[session_id] = state
            logger.info("Registered audio session %s. Active sessions: %d", session_id, len(self.sessions))
            return state

    async def connect_upstream(self, session_id: str) -> bool:
        """Connect to downstream transcription service WebSocket for this session."""
        state = self.sessions.get(session_id)
        if not state:
            return False

        url = f"{self.transcription_ws_url}/{session_id}"
        try:
            upstream = await websockets.connect(url, max_size=10 * 1024 * 1024)
            state.upstream_ws = upstream
            state.upstream_connected = True
            logger.info("Connected session %s upstream to %s", session_id, url)
            return True
        except Exception as exc:
            logger.warning(
                "Could not connect session %s upstream to %s: %s (will buffer chunks)",
                session_id,
                url,
                exc,
            )
            state.upstream_connected = False
            state.upstream_ws = None
            return False

    async def forward_audio(self, session_id: str, chunk: bytes) -> bool:
        """Buffer chunk and forward to upstream transcription service."""
        state = self.sessions.get(session_id)
        if not state or not state.is_active:
            return False

        # 1. Enqueue chunk into bounded session buffer
        await state.buffer.push(chunk)
        state.chunks_received += 1

        # 2. Check upstream connection
        if not state.upstream_connected or state.upstream_ws is None:
            # Attempt lazy reconnect
            connected = await self.connect_upstream(session_id)
            if not connected:
                return False

        # 3. Drain and forward chunk(s)
        try:
            t0 = time.perf_counter()
            # Send raw binary chunk to transcription service
            await state.upstream_ws.send(chunk)
            dt_ms = (time.perf_counter() - t0) * 1000.0

            state.chunks_forwarded += 1
            state.total_forward_latency_ms += dt_ms
            return True
        except (websockets.ConnectionClosed, websockets.WebSocketException) as exc:
            logger.warning("Upstream connection lost for session %s: %s", session_id, exc)
            state.upstream_connected = False
            state.upstream_ws = None
            return False
        except Exception as exc:
            logger.error("Error forwarding audio for session %s: %s", session_id, exc)
            return False

    async def disconnect_session(self, session_id: str) -> None:
        """Clean up session on client disconnection."""
        async with self._lock:
            state = self.sessions.pop(session_id, None)
            if state:
                await self._cleanup_session(state)
                logger.info(
                    "Disconnected audio session %s. Remaining sessions: %d",
                    session_id,
                    len(self.sessions),
                )

    async def _cleanup_session(self, state: SessionState) -> None:
        """Internal helper to close upstream connection and free buffers."""
        state.is_active = False
        if state.upstream_ws is not None:
            try:
                # Send end-of-stream control message if protocol supports it
                await state.upstream_ws.send(b"__EOS__")
                await state.upstream_ws.close()
            except Exception:
                pass
            state.upstream_ws = None
            state.upstream_connected = False

        await state.buffer.clear()

    def get_session_stats(self, session_id: str) -> Optional[Dict[str, Any]]:
        state = self.sessions.get(session_id)
        if not state:
            return None

        avg_latency = (
            state.total_forward_latency_ms / state.chunks_forwarded
            if state.chunks_forwarded > 0
            else 0.0
        )
        return {
            "session_id": session_id,
            "connected_at": state.connected_at.isoformat(),
            "buffered_duration_seconds": state.buffer.duration_seconds,
            "buffered_bytes": state.buffer.current_bytes,
            "total_bytes_received": state.buffer.total_bytes_received,
            "total_bytes_dropped": state.buffer.total_bytes_dropped,
            "chunks_received": state.chunks_received,
            "chunks_forwarded": state.chunks_forwarded,
            "avg_forward_latency_ms": round(avg_latency, 3),
            "upstream_connected": state.upstream_connected,
        }

    def get_global_stats(self) -> Dict[str, Any]:
        total_received = sum(s.chunks_received for s in self.sessions.values())
        total_forwarded = sum(s.chunks_forwarded for s in self.sessions.values())
        total_latency = sum(s.total_forward_latency_ms for s in self.sessions.values())
        avg_latency = (
            total_latency / total_forwarded if total_forwarded > 0 else 0.0
        )
        return {
            "active_sessions_count": len(self.sessions),
            "total_chunks_received": total_received,
            "total_chunks_forwarded": total_forwarded,
            "avg_forward_latency_ms": round(avg_latency, 3),
        }
