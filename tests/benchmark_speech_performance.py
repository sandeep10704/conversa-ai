"""Performance and Latency Benchmark for Conversa AI Speech Pipeline.

Measures:
1. Audio ingestion latency (buffer push & frame slicing).
2. VAD processing latency (Silero VAD per 32ms frame).
3. Transcription latency (faster-whisper inference on CPU).
4. Partial transcript latency.
5. Final transcript latency (end-to-end VAD silence detection to finalized transcript).
6. Memory consumption.
"""

import asyncio
import importlib.util
import os
import resource
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent


def _load_mod(name: str, rel_path: str):
    path = str(BASE_DIR / rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Load service modules
_mod_buf = _load_mod("ag.buffer", "services/audio-gateway/app/buffer.py")
sys.modules["app.buffer"] = _mod_buf
_mod_cap = _load_mod("ag.capture", "services/audio-gateway/app/capture.py")
SessionAudioBuffer = _mod_buf.SessionAudioBuffer
AudioGatewayManager = _mod_cap.AudioGatewayManager

_mod_vad = _load_mod("ts.vad", "services/transcription-service/app/vad.py")
sys.modules["app.vad"] = _mod_vad
_mod_trans = _load_mod("ts.transcriber", "services/transcription-service/app/transcriber.py")
sys.modules["app.transcriber"] = _mod_trans
_mod_seg = _load_mod("ts.segment_manager", "services/transcription-service/app/segment_manager.py")
SileroVAD = _mod_vad.SileroVAD
WhisperTranscriber = _mod_trans.WhisperTranscriber
SpeechSegmentManager = _mod_seg.SpeechSegmentManager

_mod_del_ws = _load_mod("ds.websocket", "services/delivery-service/app/websocket.py")
DeliveryConnectionManager = _mod_del_ws.DeliveryConnectionManager


async def run_benchmark():
    print("=" * 70)
    print("Conversa AI Speech Pipeline Performance & Latency Benchmark")
    print("=" * 70)

    # 1. Measure Audio Ingestion Latency
    buffer = SessionAudioBuffer(session_id="benchmark-session")
    chunk_100ms = b"\x00\x00" * 1600  # 100ms PCM 16kHz
    ingestion_latencies = []

    for _ in range(500):
        t0 = time.perf_counter()
        await buffer.push(chunk_100ms)
        await buffer.pop_chunk(1024)
        dt = (time.perf_counter() - t0) * 1000.0
        ingestion_latencies.append(dt)

    p50_ingestion = np.percentile(ingestion_latencies, 50)
    p95_ingestion = np.percentile(ingestion_latencies, 95)
    p99_ingestion = np.percentile(ingestion_latencies, 99)
    print(f"1. Audio Ingestion Latency (500 chunks, 100ms each):")
    print(f"   P50: {p50_ingestion:.3f} ms | P95: {p95_ingestion:.3f} ms | P99: {p99_ingestion:.3f} ms")

    # 2. Measure VAD Processing Latency (Silero VAD)
    vad = SileroVAD(threshold=0.5, mock_mode=False)
    frame_32ms = (np.sin(np.linspace(0, 5, 512)) * 10000).astype(np.int16).tobytes()
    vad_latencies = []

    # Warmup
    for _ in range(10):
        vad.process_chunk(frame_32ms)

    for _ in range(200):
        t0 = time.perf_counter()
        vad.process_chunk(frame_32ms)
        dt = (time.perf_counter() - t0) * 1000.0
        vad_latencies.append(dt)

    p50_vad = np.percentile(vad_latencies, 50)
    p95_vad = np.percentile(vad_latencies, 95)
    p99_vad = np.percentile(vad_latencies, 99)
    print(f"\n2. VAD Processing Latency per 32ms frame (Silero VAD on CPU):")
    print(f"   P50: {p50_vad:.3f} ms | P95: {p95_vad:.3f} ms | P99: {p99_vad:.3f} ms")
    print(f"   Real-time factor: {p50_vad / 32.0:.4f}x (buffer is 32ms, process is {p50_vad:.2f}ms)")

    # 3. Measure Whisper Transcription Latency
    fixture_path = BASE_DIR / "tests" / "fixtures" / "audio" / "sample_utterance.pcm"
    with open(fixture_path, "rb") as f:
        audio_sample = f.read()

    transcriber = WhisperTranscriber(model_size="tiny.en", device="cpu", compute_type="int8", mock_mode=False)
    asr_latencies = []

    # Warmup
    transcriber.transcribe(audio_sample[:32000])

    # Run 5 inferences on ~2.5s speech audio
    for _ in range(5):
        res = transcriber.transcribe(audio_sample)
        asr_latencies.append(res.latency_ms)

    avg_asr = np.mean(asr_latencies)
    audio_duration_sec = len(audio_sample) / 32000.0
    rtf_asr = (avg_asr / 1000.0) / audio_duration_sec
    print(f"\n3. Transcription Latency (faster-whisper tiny.en CPU int8, duration: {audio_duration_sec:.2f}s):")
    print(f"   Mean latency: {avg_asr:.1f} ms | Real-Time Factor (RTF): {rtf_asr:.3f}")

    # 4. Measure Partial Transcript Latency
    partial_latencies = []
    for _ in range(5):
        res_partial = transcriber.transcribe(audio_sample[:16000], is_final=False)  # 0.5s audio
        partial_latencies.append(res_partial.latency_ms)

    avg_partial = np.mean(partial_latencies)
    print(f"\n4. Partial Transcript Latency (0.5s slice):")
    print(f"   Mean latency: {avg_partial:.1f} ms")

    # 5. Measure End-to-End Final Transcript Latency
    manager = SpeechSegmentManager(
        vad=vad,
        transcriber=transcriber,
        min_speech_duration_ms=150.0,
        min_silence_duration_ms=300.0,
    )
    session_id = str(uuid4())
    manager._post_to_agent_server = AsyncMock(return_value={"sequence_number": 1})
    manager._dispatch_to_delivery_service = AsyncMock()

    t_start = time.perf_counter()
    chunk_size = 3200  # 100ms
    final_res = []
    for i in range(0, len(audio_sample), chunk_size):
        chunk = audio_sample[i : i + chunk_size]
        res = await manager.process_audio_chunk(session_id, chunk)
        final_res.extend(res)

    flush_res = await manager.flush_session(session_id)
    final_res.extend(flush_res)
    total_pipeline_time_ms = (time.perf_counter() - t_start) * 1000.0

    print(f"\n5. End-to-End Pipeline Execution Time (streaming entire 2.5s audio sample):")
    print(f"   Total processing time: {total_pipeline_time_ms:.1f} ms for 2,500 ms audio stream")
    print(f"   Pipeline RTF: {total_pipeline_time_ms / 2500.0:.3f}")

    # 6. Memory Consumption
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss_mb = rusage.ru_maxrss / (1024.0 * 1024.0) if sys.platform == "darwin" else rusage.ru_maxrss / 1024.0
    print(f"\n6. Memory Consumption:")
    print(f"   Peak Resident Set Size (RSS): {max_rss_mb:.1f} MB")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_benchmark())
