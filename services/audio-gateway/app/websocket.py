"""WebSocket endpoint handler for client audio streaming.

Accepts 16kHz 16-bit mono PCM chunks from clients and forwards them
to the Transcription Service while maintaining session state.
"""

import json
import logging
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

from app.capture import AudioGatewayManager

logger = logging.getLogger(__name__)

router = APIRouter()
gateway_manager = AudioGatewayManager()


@router.websocket("/ws/audio/{session_id}")
async def audio_websocket_endpoint(websocket: WebSocket, session_id: str) -> None:
    """Live audio streaming WebSocket endpoint.

    Expected audio format:
    - Sample rate: 16,000 Hz
    - Channels: 1 (Mono)
    - Encoding: 16-bit Signed Integer Little-Endian (PCM_S16LE)
    - Recommended chunk duration: 20ms (640 bytes) to 100ms (3200 bytes)
    """
    # 1. Validate session_id format
    try:
        UUID(session_id)
    except ValueError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid session_id UUID")
        return

    await websocket.accept()

    # 2. Register session in manager
    state = await gateway_manager.register_session(session_id, websocket)

    # 3. Connect upstream to transcription service
    upstream_ready = await gateway_manager.connect_upstream(session_id)

    # 4. Send handshake acknowledgement
    await websocket.send_text(
        json.dumps(
            {
                "status": "ready",
                "session_id": session_id,
                "upstream_connected": upstream_ready,
                "audio_contract": {
                    "sample_rate": 16000,
                    "channels": 1,
                    "format": "pcm_s16le",
                    "chunk_size_bytes_recommended": 1024,
                },
            }
        )
    )

    try:
        while True:
            message = await websocket.receive()
            if "bytes" in message and message["bytes"]:
                # Binary audio chunk
                raw_audio = message["bytes"]
                await gateway_manager.forward_audio(session_id, raw_audio)

            elif "text" in message and message["text"]:
                try:
                    payload = json.loads(message["text"])
                    msg_type = payload.get("type")
                    if msg_type == "ping":
                        await websocket.send_text(json.dumps({"type": "pong", "session_id": session_id}))
                    elif msg_type == "stop":
                        logger.info("Received stop command from client for session %s", session_id)
                        break
                except json.JSONDecodeError:
                    pass

    except WebSocketDisconnect:
        logger.info("Client disconnected from audio session %s", session_id)
    except Exception as exc:
        logger.error("Unexpected error in audio WebSocket for session %s: %s", session_id, exc)
    finally:
        await gateway_manager.disconnect_session(session_id)
