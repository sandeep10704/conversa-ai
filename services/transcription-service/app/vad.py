"""Voice Activity Detection (VAD) module using Silero VAD.

Processes 16kHz mono audio frames and calculates speech probabilities
to segment continuous audio into discrete speech utterances.
"""

import logging
import os
import time
from typing import Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


class SileroVAD:
    """Wrapper around Silero VAD with optional mock mode and CPU optimizations."""

    FRAME_SIZE_SAMPLES = 512  # 32ms at 16,000 Hz
    FRAME_SIZE_BYTES = 512 * 2  # 1024 bytes for 16-bit PCM

    def __init__(
        self,
        threshold: float = 0.5,
        sample_rate: int = 16000,
        mock_mode: Optional[bool] = None,
    ) -> None:
        self.threshold = threshold
        self.sample_rate = sample_rate
        self.mock_mode = (
            mock_mode
            if mock_mode is not None
            else os.getenv("VAD_MOCK", "false").lower() in ("true", "1", "yes")
        )
        self._model = None
        self._torch = None
        self._total_frames_processed = 0
        self._total_processing_time_ms = 0.0

        if not self.mock_mode:
            self._load_model()

    def _load_model(self) -> None:
        """Load Silero VAD model."""
        try:
            import silero_vad
            import torch

            self._torch = torch
            self._model = silero_vad.load_silero_vad(onnx=False)
            logger.info("Silero VAD model loaded successfully on CPU.")
        except Exception as exc:
            logger.warning(
                "Failed to load Silero VAD model (%s); falling back to energy-based VAD.",
                exc,
            )
            self.mock_mode = True

    def process_chunk(self, chunk_pcm16: bytes) -> Tuple[bool, float, float]:
        """Process a PCM 16-bit 16kHz audio chunk.

        Returns:
            Tuple of (is_speech: bool, speech_probability: float, processing_latency_ms: float)
        """
        t0 = time.perf_counter()

        if len(chunk_pcm16) < self.FRAME_SIZE_BYTES:
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return False, 0.0, dt_ms

        if self.mock_mode:
            # Energy-based speech detector for mock/testing
            samples = np.frombuffer(chunk_pcm16[: self.FRAME_SIZE_BYTES], dtype=np.int16)
            rms = np.sqrt(np.mean(samples.astype(np.float32) ** 2))
            # RMS threshold: silent audio ~ 0-50, speech ~ 500-15000
            prob = float(min(1.0, rms / 1000.0))
            is_speech = prob >= self.threshold
            dt_ms = (time.perf_counter() - t0) * 1000.0
            self._total_frames_processed += 1
            self._total_processing_time_ms += dt_ms
            return is_speech, prob, dt_ms

        try:
            # Convert PCM 16-bit to normalized float32 tensor
            samples = np.frombuffer(chunk_pcm16[: self.FRAME_SIZE_BYTES], dtype=np.int16)
            audio_float = samples.astype(np.float32) / 32768.0
            tensor = self._torch.from_numpy(audio_float)

            with self._torch.no_grad():
                prob = float(self._model(tensor, self.sample_rate).item())

            is_speech = prob >= self.threshold
            dt_ms = (time.perf_counter() - t0) * 1000.0
            self._total_frames_processed += 1
            self._total_processing_time_ms += dt_ms
            return is_speech, prob, dt_ms
        except Exception as exc:
            logger.error("Error in Silero VAD inference: %s", exc)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return False, 0.0, dt_ms

    def reset(self) -> None:
        """Reset internal recurrent states if model supports it."""
        if self._model is not None and hasattr(self._model, "reset_states"):
            self._model.reset_states()

    @property
    def avg_latency_ms(self) -> float:
        if self._total_frames_processed == 0:
            return 0.0
        return self._total_processing_time_ms / self._total_frames_processed
