"""Speech-to-Text module using faster-whisper.

Transcribes audio segments into text hypotheses with start and end timestamps.
Supports configurable CPU/GPU execution and mock mode for testing.
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class TranscriptionResult:
    """Represents the output of speech-to-text inference."""

    text: str
    start_time: float = 0.0
    end_time: float = 0.0
    duration: float = 0.0
    latency_ms: float = 0.0
    is_final: bool = True


class WhisperTranscriber:
    """ASR engine using faster-whisper with CPU int8 quantization."""

    def __init__(
        self,
        model_size: Optional[str] = None,
        device: Optional[str] = None,
        compute_type: Optional[str] = None,
        mock_mode: Optional[bool] = None,
    ) -> None:
        self.model_size = model_size or os.getenv("WHISPER_MODEL_SIZE", "tiny.en")
        self.device = device or os.getenv("WHISPER_DEVICE", "cpu")
        self.compute_type = compute_type or os.getenv("WHISPER_COMPUTE_TYPE", "int8")
        self.mock_mode = (
            mock_mode
            if mock_mode is not None
            else os.getenv("WHISPER_MOCK", "false").lower() in ("true", "1", "yes")
        )
        self._model = None
        self._total_inferences = 0
        self._total_transcription_time_ms = 0.0
        self._mock_text_override: Optional[str] = None

        if not self.mock_mode:
            self._load_model()

    def set_mock_text(self, text: Optional[str]) -> None:
        """Override text returned during mock mode."""
        self._mock_text_override = text

    def _load_model(self) -> None:
        """Load faster-whisper model."""
        try:
            from faster_whisper import WhisperModel

            logger.info(
                "Loading faster-whisper model '%s' (device=%s, compute_type=%s)...",
                self.model_size,
                self.device,
                self.compute_type,
            )
            self._model = WhisperModel(
                self.model_size,
                device=self.device,
                compute_type=self.compute_type,
                cpu_threads=4,
            )
            logger.info("faster-whisper model loaded successfully.")
        except Exception as exc:
            logger.warning(
                "Failed to load faster-whisper model (%s); falling back to mock mode.",
                exc,
            )
            self.mock_mode = True

    def transcribe(
        self,
        audio_bytes: bytes,
        sample_rate: int = 16000,
        is_final: bool = True,
    ) -> TranscriptionResult:
        """Transcribe raw PCM 16-bit mono audio into text."""
        t0 = time.perf_counter()

        if not audio_bytes or len(audio_bytes) < 320:  # < 10ms
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return TranscriptionResult(
                text="",
                start_time=0.0,
                end_time=0.0,
                duration=0.0,
                latency_ms=dt_ms,
                is_final=is_final,
            )

        duration = len(audio_bytes) / (sample_rate * 2)

        if self.mock_mode:
            dt_ms = (time.perf_counter() - t0) * 1000.0
            self._total_inferences += 1
            self._total_transcription_time_ms += dt_ms
            text = (
                self._mock_text_override
                if self._mock_text_override is not None
                else "I think we should investigate the latency spike on the database."
            )
            return TranscriptionResult(
                text=text,
                start_time=0.0,
                end_time=round(duration, 3),
                duration=round(duration, 3),
                latency_ms=round(dt_ms, 2),
                is_final=is_final,
            )

        try:
            # Convert PCM 16-bit bytes to float32 numpy array
            samples = np.frombuffer(audio_bytes, dtype=np.int16).astype(np.float32) / 32768.0

            segments, _ = self._model.transcribe(
                samples,
                beam_size=1,
                language="en",
                condition_on_previous_text=False,
                vad_filter=False,  # VAD is handled independently by Silero
            )

            text_parts = []
            start_time = 0.0
            end_time = duration

            for seg in segments:
                text_parts.append(seg.text.strip())
                if start_time == 0.0 and seg.start is not None:
                    start_time = seg.start
                if seg.end is not None:
                    end_time = seg.end

            full_text = " ".join(text_parts).strip()
            dt_ms = (time.perf_counter() - t0) * 1000.0

            self._total_inferences += 1
            self._total_transcription_time_ms += dt_ms

            return TranscriptionResult(
                text=full_text,
                start_time=round(start_time, 3),
                end_time=round(end_time, 3),
                duration=round(duration, 3),
                latency_ms=round(dt_ms, 2),
                is_final=is_final,
            )
        except Exception as exc:
            logger.error("Error during Whisper transcription: %s", exc)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return TranscriptionResult(
                text="",
                start_time=0.0,
                end_time=round(duration, 3),
                duration=round(duration, 3),
                latency_ms=round(dt_ms, 2),
                is_final=is_final,
            )

    @property
    def avg_latency_ms(self) -> float:
        if self._total_inferences == 0:
            return 0.0
        return self._total_transcription_time_ms / self._total_inferences
