# Conversa AI: Audio Format & Ingestion Contract

This document specifies the technical audio contract for streaming real-time audio from client applications into the Conversa AI **Audio Gateway** and downstream **Transcription Service**.

---

## 1. Core Audio Specification

All audio ingested through the Conversa AI Audio Gateway must adhere to the following uncompressed Linear PCM contract:

| Parameter | Specification | Technical Rationale |
| :--- | :--- | :--- |
| **Encoding / Codec** | Linear PCM (Pulse Code Modulation), Uncompressed | Eliminates decoding latency and CPU overhead on gateway instances. |
| **Sample Rate** | `16,000 Hz` (16 kHz) | Standard native sample rate for Silero VAD v4/v5 and OpenAI Whisper acoustic feature extraction (80-channel log-mel spectrogram). |
| **Channels** | `1` (Mono) | Conversations are processed as a single chronological discussion stream without spatial separation. |
| **Bit Depth / Sample Representation** | `16-bit signed integer` (`int16`, `pcm_s16le`) | Two's complement integer representation in Little-Endian byte order. Range: `[-32,768, 32,767]`. |
| **Byte Order** | Little-Endian (`<h` / `little`) | Standard IEEE x86_64 and ARM64 architecture byte order. |
| **Bytes Per Sample** | `2 bytes` | 16 bits = 2 bytes per sample. |
| **Bitrate / Data Rate** | `256 kbps` (`32,000 bytes/sec`) | $16,000 \text{ samples/sec} \times 2 \text{ bytes} = 32 \text{ KB/s}$. |

---

## 2. Chunk Framing & Streaming Parameters

Clients stream audio incrementally over WebSockets (`ws://<host>:8002/ws/audio/{session_id}`).

### Framing Guidelines

| Frame Size (ms) | Samples | Bytes | Ingestion Latency | Recommended Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **20 ms** | 320 | 640 B | ~0.001 ms | Low-latency mobile/browser microphone capture |
| **32 ms** | 512 | 1,024 B | ~0.001 ms | **Native Silero VAD frame size** (optimal alignment) |
| **50 ms** | 800 | 1,600 B | ~0.002 ms | High-throughput browser streaming |
| **100 ms** | 1,600 | 3,200 B | ~0.003 ms | Standard network-efficient chunking |

> **Frame Normalization:** The Audio Gateway maintains a bounded session buffer (`SessionAudioBuffer`) that slices incoming arbitrary-sized byte chunks into the canonical **512-sample (1,024-byte)** frames expected by the Silero Voice Activity Detector.

---

## 3. WebSocket Protocol & Handshake

### Endpoint
`ws://<audio-gateway-host>:8002/ws/audio/{session_id}`

### Validation
- `session_id` must be a valid RFC 4122 UUID matching an existing or newly initialized conversation session. Non-UUID paths are rejected with WebSocket close code `1008` (Policy Violation).

### Handshake Sequence
1. Client establishes WebSocket connection.
2. Audio Gateway validates `session_id`, allocates per-session buffer, and connects upstream to `ws://transcription-service:8004/ws/transcribe/{session_id}`.
3. Audio Gateway emits ready acknowledgment text frame:
   ```json
   {
     "status": "ready",
     "session_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
     "upstream_connected": true,
     "audio_contract": {
       "sample_rate": 16000,
       "channels": 1,
       "format": "pcm_s16le",
       "chunk_size_bytes_recommended": 1024
     }
   }
   ```
4. Client continuously sends binary WebSocket frames containing raw PCM bytes.
5. On discussion termination, client optionally sends text frame:
   ```json
   { "type": "stop" }
   ```
   or cleanly closes the WebSocket.

---

## 4. Voice Activity Detection & Utterance Segmentation

The Transcription Service executes continuous Voice Activity Detection:

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| **VAD Engine** | Silero VAD (PyTorch / ONNX) | Deep learning VAD operating on 512-sample float32 frames |
| **Speech Threshold** | `0.5` | Probability threshold above which a frame is classified as speech |
| **Minimum Speech Duration** | `250 ms` (approx. 8 frames) | Discards clicks, coughs, and short acoustic transients |
| **Silence Finalization Threshold** | `600 ms` (approx. 19 frames) | Silence duration required to declare an utterance completed |
| **Partial Hypothesis Interval** | `500 ms` | Interval between emitting interim streaming partial transcripts |

---

## 5. Architectural Invariants

1. **No Binary Audio in Kafka:** Audio chunks are strictly streamed peer-to-peer over internal WebSockets between `audio-gateway` and `transcription-service`. Apache Kafka topics convey only structured JSON event metadata.
2. **No Raw Audio in PostgreSQL:** Audio is buffered transiently in RAM (bounded to max 30 seconds per session with automatic tail-drop protection) and never persisted to the relational database.
3. **No Speaker Diarization / Identification:** Transcripts represent chronological conversation flow without speaker labels.
4. **Single Source of Truth for Messages:** Finalized transcript segments are posted to `POST /v1/sessions/{session_id}/messages` on the Agent Server, which atomically commits the message, assigns sequence numbers, and writes the Transactional Outbox event.
