"""Audio stream buffering utilities for Conversa AI Audio Gateway.

Provides bounded, thread-safe buffering for raw PCM 16-bit 16kHz mono audio
streams to handle client burstiness and network jitter without memory blowup.
"""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SessionAudioBuffer:
    """Bounded in-memory audio ring buffer for a single conversation session.

    Enforces memory bounds by calculating byte limits based on maximum duration.
    For 16kHz 16-bit mono audio:
        1 second = 16,000 samples * 2 bytes = 32,000 bytes.
    """

    def __init__(
        self,
        session_id: str,
        sample_rate: int = 16000,
        channels: int = 1,
        sample_width: int = 2,
        max_buffer_seconds: float = 30.0,
    ) -> None:
        self.session_id = session_id
        self.sample_rate = sample_rate
        self.channels = channels
        self.sample_width = sample_width
        self.bytes_per_second = sample_rate * channels * sample_width
        self.max_bytes = int(max_buffer_seconds * self.bytes_per_second)

        self._buffer = bytearray()
        self._lock = asyncio.Lock()
        self._total_bytes_received: int = 0
        self._total_bytes_dropped: int = 0

    @property
    def current_bytes(self) -> int:
        return len(self._buffer)

    @property
    def duration_seconds(self) -> float:
        return len(self._buffer) / self.bytes_per_second

    @property
    def total_bytes_received(self) -> int:
        return self._total_bytes_received

    @property
    def total_bytes_dropped(self) -> int:
        return self._total_bytes_dropped

    async def push(self, data: bytes) -> int:
        """Append incoming PCM audio chunk to the buffer.

        If buffer capacity is exceeded, drops oldest data to enforce max duration.
        Returns number of bytes accepted into the buffer.
        """
        if not data:
            return 0

        async with self._lock:
            self._total_bytes_received += len(data)
            self._buffer.extend(data)

            if len(self._buffer) > self.max_bytes:
                overflow = len(self._buffer) - self.max_bytes
                # Drop oldest bytes to maintain real-time window
                del self._buffer[:overflow]
                self._total_bytes_dropped += overflow
                logger.warning(
                    "Session %s buffer overflow: dropped %d oldest bytes",
                    self.session_id,
                    overflow,
                )

            return len(data)

    async def pop_chunk(self, chunk_size: int = 1024) -> Optional[bytes]:
        """Extract a fixed-size chunk from the front of the buffer.

        Returns None if buffer contains fewer than chunk_size bytes.
        """
        async with self._lock:
            if len(self._buffer) < chunk_size:
                return None
            chunk = bytes(self._buffer[:chunk_size])
            del self._buffer[:chunk_size]
            return chunk

    async def drain(self) -> bytes:
        """Extract and clear all remaining audio bytes in the buffer."""
        async with self._lock:
            all_data = bytes(self._buffer)
            self._buffer.clear()
            return all_data

    async def clear(self) -> None:
        """Clear all buffered data."""
        async with self._lock:
            self._buffer.clear()
