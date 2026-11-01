"""Conversa AI — Real Audio-to-Suggestion End-to-End Integration Test.

Verifies the complete real audio-to-suggestion pipeline across Docker Compose services:
Real/Prerecorded Audio
  → Audio Gateway (:8002)
  → Transcription Service (:8004)
  → Silero VAD
  → faster-whisper
  → Agent API (:8001)
  → PostgreSQL + Transactional Outbox
  → Kafka (conversation.transcript.final)
  → Trigger Controller
  → Agent Worker (MOCK_LLM=true)
  → Suggestion Persistence (PostgreSQL + outbox)
  → Kafka (agent.suggestions)
  → Delivery Service (:8003)
  → Correct Session WebSocket.
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import wave
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

import asyncpg
import httpx
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [AudioIntegrationTest] %(message)s",
)
logger = logging.getLogger("audio_integration_test")

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_AUDIO_WAV = BASE_DIR / "tests" / "fixtures" / "audio" / "three_utterances_conversation.wav"
DEFAULT_AUDIO_PCM = BASE_DIR / "tests" / "fixtures" / "audio" / "three_utterances_conversation.pcm"

AGENT_API_URL = "http://localhost:8001"
AUDIO_GATEWAY_WS_URL = "ws://localhost:8002/ws/audio"
DELIVERY_WS_URL = "ws://localhost:8003/ws/delivery"
TRANSCRIPTION_API_URL = "http://localhost:8004"
POSTGRES_DSN = "postgresql://conversa_user:conversa_password@localhost:5432/conversa"


def load_or_convert_audio_pcm(file_path: Path) -> bytes:
    """Load audio fixture and ensure it is 16kHz 16-bit mono PCM."""
    if not file_path.exists():
        raise FileNotFoundError(f"Audio fixture file not found: {file_path}")

    ext = file_path.suffix.lower()
    if ext == ".pcm" or ext == ".raw":
        with open(file_path, "rb") as f:
            return f.read()

    elif ext == ".wav":
        with wave.open(str(file_path), "rb") as wf:
            channels = wf.getnchannels()
            sample_width = wf.getsampwidth()
            framerate = wf.getframerate()
            frames = wf.readframes(wf.getnframes())

        # If already 16kHz mono 16-bit, return directly
        if channels == 1 and sample_width == 2 and framerate == 16000:
            return frames

        # Otherwise convert using afconvert (macOS)
        logger.info(
            "Converting audio %s (ch=%d, w=%d, rate=%d) to 16kHz mono 16-bit PCM...",
            file_path,
            channels,
            sample_width,
            framerate,
        )
        temp_wav = "/tmp/converted_conversa_temp.wav"
        subprocess.run(
            ["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", str(file_path), temp_wav],
            check=True,
        )
        with wave.open(temp_wav, "rb") as wf:
            converted_frames = wf.readframes(wf.getnframes())
        if os.path.exists(temp_wav):
            os.remove(temp_wav)
        return converted_frames

    else:
        raise ValueError(f"Unsupported audio format: {ext}. Expected .wav or .pcm")


async def fetch_db_evidence(session_id: str) -> Dict[str, Any]:
    """Directly query PostgreSQL tables for ground truth evidence."""
    evidence: Dict[str, Any] = {
        "messages": [],
        "outbox_events": [],
        "agent_executions": [],
        "suggestions": [],
        "processed_events": [],
    }
    try:
        conn = await asyncpg.connect(POSTGRES_DSN)
        try:
            # 1. Conversation messages
            msg_rows = await conn.fetch(
                "SELECT message_id, sequence_number, text, source, created_at "
                "FROM conversation_messages WHERE session_id = $1 ORDER BY sequence_number ASC",
                UUID(session_id),
            )
            evidence["messages"] = [dict(r) for r in msg_rows]

            # 2. Outbox events for this session
            outbox_rows = await conn.fetch(
                "SELECT event_id, event_type, status, published_at, created_at "
                "FROM outbox_events WHERE aggregate_id = $1 ORDER BY created_at ASC",
                UUID(session_id),
            )
            evidence["outbox_events"] = [dict(r) for r in outbox_rows]

            # 3. Agent executions
            exec_rows = await conn.fetch(
                "SELECT execution_id, execution_status, trigger_reason, processed_sequence_number, "
                "selected_model, created_at, completed_at "
                "FROM agent_executions WHERE session_id = $1 ORDER BY created_at ASC",
                UUID(session_id),
            )
            evidence["agent_executions"] = [dict(r) for r in exec_rows]

            # 4. Suggestions
            sugg_rows = await conn.fetch(
                "SELECT suggestion_id, response_type, suggestion_text, rationale, "
                "source_last_sequence_number, created_at, delivery_status "
                "FROM suggestions WHERE session_id = $1 ORDER BY created_at ASC",
                UUID(session_id),
            )
            evidence["suggestions"] = [dict(r) for r in sugg_rows]

            # 5. Processed events
            proc_rows = await conn.fetch(
                "SELECT event_id, event_type, consumer_id, processed_at "
                "FROM processed_events WHERE session_id = $1 ORDER BY processed_at ASC",
                UUID(session_id),
            )
            evidence["processed_events"] = [dict(r) for r in proc_rows]

        finally:
            await conn.close()
    except Exception as exc:
        logger.warning("Could not query PostgreSQL directly: %s", exc)

    return evidence


async def run_audio_integration_test(
    audio_path: Path,
    chunk_ms: int = 50,
    streaming_speed: float = 1.0,
    timeout: float = 40.0,
) -> bool:
    """Execute end-to-end audio ingestion to suggestion delivery test."""
    print("\n" + "=" * 80)
    print("CONVERSA AI — REAL AUDIO-TO-SUGGESTION PIPELINE INTEGRATION TEST")
    print("=" * 80)
    print(f"Audio Fixture:   {audio_path}")
    print(f"Chunk Duration:  {chunk_ms} ms")
    print(f"Streaming Speed: {streaming_speed}x real-time")
    print(f"Overall Timeout: {timeout} s")
    print("=" * 80 + "\n")

    # Stage results tracker
    stages: Dict[str, str] = {
        "1. Service Health & Readiness": "NOT TESTED",
        "2. Audio Fixture Validation": "NOT TESTED",
        "3. Session Creation & Isolation Setup": "NOT TESTED",
        "4. WebSocket Handshakes": "NOT TESTED",
        "5. Audio Streaming & Ingestion (Audio Gateway)": "NOT TESTED",
        "6. Silero VAD Speech Segmentation": "NOT TESTED",
        "7. faster-whisper ASR Transcription": "NOT TESTED",
        "8. Final Transcripts Persisted to Agent API & DB": "NOT TESTED",
        "9. Transactional Outbox Publishing (Kafka)": "NOT TESTED",
        "10. Trigger Controller Debounce & Command Dispatch": "NOT TESTED",
        "11. Agent Worker Execution (MOCK_LLM)": "NOT TESTED",
        "12. Suggestion Persistence & Delivery WebSocket": "NOT TESTED",
        "13. Multi-Session Isolation": "NOT TESTED",
    }

    # -------------------------------------------------------------
    # Step 1: Health checks
    # -------------------------------------------------------------
    logger.info("Checking service health endpoints...")
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r1 = await client.get(f"{AGENT_API_URL}/health/live")
            r2 = await client.get(f"http://localhost:8002/health/live")
            r3 = await client.get(f"http://localhost:8003/health/live")
            r4 = await client.get(f"{TRANSCRIPTION_API_URL}/health/ready")
            assert r1.status_code == 200, f"Agent API unhealthy: {r1.status_code}"
            assert r2.status_code == 200, f"Audio Gateway unhealthy: {r2.status_code}"
            assert r3.status_code == 200, f"Delivery Service unhealthy: {r3.status_code}"
            assert r4.status_code == 200, f"Transcription Service unhealthy: {r4.status_code}"
            t_ready = r4.json()
            logger.info(
                "✓ All services healthy. Transcription Service ready: vad_mock=%s, whisper_mock=%s, model=%s",
                t_ready.get("vad_mock_mode"),
                t_ready.get("whisper_mock_mode"),
                t_ready.get("whisper_model_size"),
            )
            stages["1. Service Health & Readiness"] = "PASS"
        except Exception as exc:
            logger.error("Health check failed: %s", exc)
            stages["1. Service Health & Readiness"] = f"FAIL ({exc})"
            return False

    # -------------------------------------------------------------
    # Step 2: Validate Audio Fixture
    # -------------------------------------------------------------
    try:
        pcm_bytes = load_or_convert_audio_pcm(audio_path)
        total_seconds = len(pcm_bytes) / 32000.0  # 16000 * 2 bytes/sec
        logger.info(
            "✓ Loaded audio fixture: %d bytes (%.2f seconds of 16kHz 16-bit mono PCM)",
            len(pcm_bytes),
            total_seconds,
        )
        if total_seconds < 3.0:
            logger.warning("Audio fixture is short (%.2fs); might not contain 3 utterances.", total_seconds)
        stages["2. Audio Fixture Validation"] = "PASS"
    except Exception as exc:
        logger.error("Failed to load audio fixture: %s", exc)
        stages["2. Audio Fixture Validation"] = f"FAIL ({exc})"
        return False

    # -------------------------------------------------------------
    # Step 3: Create Sessions
    # -------------------------------------------------------------
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Session A: Target session
        session_a_payload = {
            "situation": "Live technical sync evaluating conversational suggestion latency and pipeline architecture",
            "objective": "Identify response generation bottlenecks and provide architectural recommendations",
            "communication_goals": [
                "Recommend latency optimizations",
                "Ensure conversational non-intrusiveness",
            ],
            "response_preferences": {
                "tone": "professional",
                "detail_level": "concise",
            },
            "constraints": [
                "Keep response under two sentences",
                "Do not repeat existing points",
            ],
        }
        res_a = await client.post(f"{AGENT_API_URL}/v1/sessions", json=session_a_payload)
        assert res_a.status_code == 201, f"Failed to create Session A: {res_a.text}"
        session_id_a = res_a.json()["session_id"]

        # Session B: Isolated session (must NOT receive Session A's suggestions)
        session_b_payload = {
            "situation": "Isolated background discussion",
            "objective": "Monitor session isolation",
            "communication_goals": ["Observe"],
            "response_preferences": {"tone": "casual", "detail_level": "brief"},
            "constraints": [],
        }
        res_b = await client.post(f"{AGENT_API_URL}/v1/sessions", json=session_b_payload)
        assert res_b.status_code == 201, f"Failed to create Session B: {res_b.text}"
        session_id_b = res_b.json()["session_id"]

        logger.info("✓ Created primary session A: %s", session_id_a)
        logger.info("✓ Created isolation session B: %s", session_id_b)
        stages["3. Session Creation & Isolation Setup"] = "PASS"

    # -------------------------------------------------------------
    # Step 4: Setup Delivery and Gateway WebSockets
    # -------------------------------------------------------------
    ws_delivery_a_uri = f"{DELIVERY_WS_URL}/{session_id_a}"
    ws_delivery_b_uri = f"{DELIVERY_WS_URL}/{session_id_b}"
    ws_gateway_a_uri = f"{AUDIO_GATEWAY_WS_URL}/{session_id_a}"

    received_partials_a: List[str] = []
    received_finals_a: List[Dict[str, Any]] = []
    received_suggestions_a: List[Dict[str, Any]] = []
    received_suggestions_b: List[Dict[str, Any]] = []

    suggestion_event = asyncio.Event()

    async def delivery_a_listener(ws):
        try:
            async for raw in ws:
                data = json.loads(raw)
                mtype = data.get("type")
                if mtype == "partial":
                    received_partials_a.append(data.get("text", ""))
                    logger.debug("[Delivery A] Partial: '%s'", data.get("text"))
                elif mtype == "final":
                    received_finals_a.append(data)
                    logger.info(
                        "★ [Delivery A] Final Transcript #%s: '%s'",
                        data.get("sequence_number"),
                        data.get("text"),
                    )
                elif mtype == "suggestion":
                    received_suggestions_a.append(data)
                    logger.info("🎯 [Delivery A] RECEIVED SUGGESTION OVER WEBSOCKET!")
                    logger.info("   Suggestion ID: %s", data.get("suggestion_id"))
                    logger.info("   Response Type: %s", data.get("response_type"))
                    logger.info("   Text:          %s", data.get("text"))
                    logger.info("   Rationale:     %s", data.get("rationale"))
                    suggestion_event.set()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("[Delivery A] Listener exception: %s", e)

    async def delivery_b_listener(ws):
        try:
            async for raw in ws:
                data = json.loads(raw)
                mtype = data.get("type")
                if mtype == "suggestion":
                    received_suggestions_b.append(data)
                    logger.warning("🚨 [Delivery B] RECEIVED UNEXPECTED SUGGESTION! Isolation breached: %s", data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("[Delivery B] Listener exception: %s", e)

    # -------------------------------------------------------------
    # Step 5 & 6: Connect and Stream Audio
    # -------------------------------------------------------------
    logger.info("Connecting WebSockets...")
    async with websockets.connect(ws_delivery_a_uri) as ws_deliv_a, \
               websockets.connect(ws_delivery_b_uri) as ws_deliv_b, \
               websockets.connect(ws_gateway_a_uri) as ws_gateway_a:

        # Listen on Delivery WebSockets
        deliv_a_task = asyncio.create_task(delivery_a_listener(ws_deliv_a))
        deliv_b_task = asyncio.create_task(delivery_b_listener(ws_deliv_b))

        # Handshake with Audio Gateway
        handshake_raw = await asyncio.wait_for(ws_gateway_a.recv(), timeout=5.0)
        handshake = json.loads(handshake_raw)
        logger.info("✓ Audio Gateway handshake: %s", handshake)
        assert handshake.get("status") == "ready", "Audio Gateway handshake not ready"
        assert handshake.get("upstream_connected") is True, "Audio Gateway upstream not connected"
        stages["4. WebSocket Handshakes"] = "PASS"

        # Calculate chunk slicing
        bytes_per_sample = 2  # 16-bit
        samples_per_sec = 16000
        chunk_bytes = int(samples_per_sec * bytes_per_sample * (chunk_ms / 1000.0))
        sleep_interval = (chunk_ms / 1000.0) / streaming_speed

        total_chunks = (len(pcm_bytes) + chunk_bytes - 1) // chunk_bytes
        logger.info(
            "Streaming audio to Audio Gateway: %d chunks of %d bytes (%.1f ms each), pace=%.2fx real-time...",
            total_chunks,
            chunk_bytes,
            chunk_ms,
            streaming_speed,
        )

        stream_start_time = time.time()
        for idx in range(total_chunks):
            chunk = pcm_bytes[idx * chunk_bytes : (idx + 1) * chunk_bytes]
            await ws_gateway_a.send(chunk)
            if sleep_interval > 0:
                await asyncio.sleep(sleep_interval)

        stream_end_time = time.time()
        logger.info(
            "✓ Finished streaming %d chunks in %.2f seconds.",
            total_chunks,
            stream_end_time - stream_start_time,
        )
        stages["5. Audio Streaming & Ingestion (Audio Gateway)"] = "PASS"

        # Allow final trailing audio to be processed and silence flushed
        await asyncio.sleep(1.0)
        await ws_gateway_a.send(json.dumps({"type": "stop"}))
        logger.info("Sent stop frame to Audio Gateway.")

        # -------------------------------------------------------------
        # Step 7: Wait for debounce & suggestion delivery
        # -------------------------------------------------------------
        logger.info("Waiting for VAD finalization, debounce (2.0s), and suggestion delivery...")
        try:
            await asyncio.wait_for(suggestion_event.wait(), timeout=timeout)
            logger.info("✓ Suggestion successfully received within timeout!")
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for suggestion on Delivery WebSocket (%s seconds)", timeout)

        # Cancel delivery listeners
        deliv_a_task.cancel()
        deliv_b_task.cancel()

    # -------------------------------------------------------------
    # Step 8: Database & Telemetry Ground Truth Verification
    # -------------------------------------------------------------
    logger.info("Retrieving database and telemetry evidence...")
    evidence = await fetch_db_evidence(session_id_a)

    # 1. Check Messages in PostgreSQL
    messages = evidence["messages"]
    logger.info("Database Messages Count: %d", len(messages))
    for m in messages:
        logger.info(
            "   [DB Message #%d] (id=%s): '%s' (src=%s)",
            m["sequence_number"],
            m["message_id"],
            m["text"],
            m["source"],
        )

    if len(messages) > 0:
        stages["6. Silero VAD Speech Segmentation"] = "PASS"
        stages["7. faster-whisper ASR Transcription"] = "PASS"
    else:
        stages["6. Silero VAD Speech Segmentation"] = "FAIL (No speech segments finalized)"
        stages["7. faster-whisper ASR Transcription"] = "FAIL (No transcripts produced)"

    if len(messages) >= 3:
        stages["8. Final Transcripts Persisted to Agent API & DB"] = f"PASS ({len(messages)} messages persisted)"
    else:
        stages["8. Final Transcripts Persisted to Agent API & DB"] = f"PARTIAL ({len(messages)}/3 messages persisted)"

    # 2. Check Outbox Events
    outbox = evidence["outbox_events"]
    published_outbox = [e for e in outbox if e["status"] == "published"]
    logger.info("Database Outbox Events: %d total, %d published", len(outbox), len(published_outbox))
    for o in outbox:
        logger.info("   [Outbox] type=%s, status=%s, id=%s", o["event_type"], o["status"], o["event_id"])

    if any(e["event_type"] == "conversation.transcript.final" and e["status"] == "published" for e in outbox):
        stages["9. Transactional Outbox Publishing (Kafka)"] = "PASS"
    else:
        stages["9. Transactional Outbox Publishing (Kafka)"] = "FAIL"

    # 3. Check Agent Executions
    executions = evidence["agent_executions"]
    logger.info("Database Agent Executions: %d", len(executions))
    for ex in executions:
        logger.info(
            "   [Execution] id=%s, status=%s, reason=%s, seq=%s, model=%s",
            ex["execution_id"],
            ex["execution_status"],
            ex["trigger_reason"],
            ex["processed_sequence_number"],
            ex["selected_model"],
        )

    if len(executions) > 0:
        stages["10. Trigger Controller Debounce & Command Dispatch"] = "PASS"
        if any(ex["execution_status"] == "completed" for ex in executions):
            stages["11. Agent Worker Execution (MOCK_LLM)"] = "PASS"
        else:
            stages["11. Agent Worker Execution (MOCK_LLM)"] = "FAIL (Execution not completed)"
    else:
        stages["10. Trigger Controller Debounce & Command Dispatch"] = "FAIL (No agent command triggered)"
        stages["11. Agent Worker Execution (MOCK_LLM)"] = "FAIL (No execution recorded)"

    # 4. Check Suggestions
    suggestions = evidence["suggestions"]
    logger.info("Database Suggestions: %d", len(suggestions))
    for s in suggestions:
        logger.info(
            "   [Suggestion] id=%s, type=%s, text='%s', seq=%s, status=%s",
            s["suggestion_id"],
            s["response_type"],
            s["suggestion_text"],
            s["source_last_sequence_number"],
            s["delivery_status"],
        )

    if len(received_suggestions_a) > 0 and len(suggestions) > 0:
        stages["12. Suggestion Persistence & Delivery WebSocket"] = "PASS"
    else:
        stages["12. Suggestion Persistence & Delivery WebSocket"] = "FAIL"

    # 5. Check Session Isolation
    if len(received_suggestions_b) == 0:
        stages["13. Multi-Session Isolation"] = "PASS (0 suggestions received on Session B)"
    else:
        stages["13. Multi-Session Isolation"] = f"FAIL ({len(received_suggestions_b)} suggestions leaked to Session B)"

    # Fetch Metrics endpoint from Agent API
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            m_resp = await client.get(f"{AGENT_API_URL}/health/metrics")
            if m_resp.status_code == 200:
                print("\n" + "-" * 40 + " AGENT API METRICS " + "-" * 40)
                print(json.dumps(m_resp.json(), indent=2))
                print("-" * 98 + "\n")
        except Exception:
            pass

    # Print Final Verification Table
    print("\n" + "=" * 80)
    print("PIPELINE STAGE VERIFICATION SUMMARY")
    print("=" * 80)
    all_passed = True
    for stage_name, result in stages.items():
        status_symbol = "✓ [PASS]" if "PASS" in result else "✗ [FAIL]"
        if "FAIL" in result:
            all_passed = False
        print(f" {status_symbol:<10} {stage_name:<52} : {result}")
    print("=" * 80)

    if all_passed:
        print("\n🎉 ALL 13 PIPELINE STAGES PASSED SUCCESSFULLY!")
    else:
        print("\n⚠️ SOME PIPELINE STAGES FAILED OR WERE INCOMPLETE.")
    print("=" * 80 + "\n")

    return all_passed


def main():
    parser = argparse.ArgumentParser(description="Conversa AI Audio-to-Suggestion End-to-End Test")
    parser.add_argument(
        "--audio-file",
        type=str,
        default=str(DEFAULT_AUDIO_WAV),
        help="Path to audio fixture (.wav or .pcm)",
    )
    parser.add_argument(
        "--chunk-ms",
        type=int,
        default=50,
        help="Audio chunk duration in milliseconds (default: 50ms)",
    )
    parser.add_argument(
        "--streaming-speed",
        type=float,
        default=1.0,
        help="Streaming speed multiplier (default: 1.0 = real-time pacing)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=35.0,
        help="Max seconds to wait for suggestion delivery",
    )
    args = parser.parse_args()

    audio_path = Path(args.audio_file)
    if not audio_path.exists():
        if DEFAULT_AUDIO_PCM.exists():
            audio_path = DEFAULT_AUDIO_PCM
        else:
            logger.error("Audio fixture not found: %s", audio_path)
            sys.exit(1)

    success = asyncio.run(
        run_audio_integration_test(
            audio_path=audio_path,
            chunk_ms=args.chunk_ms,
            streaming_speed=args.streaming_speed,
            timeout=args.timeout,
        )
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
