"""Conversa AI Delivery Service.

FastAPI entrypoint providing WebSocket endpoints for clients to receive
real-time partial transcripts, finalized transcripts, connection status,
and transcription errors with strict session isolation.
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional
from uuid import UUID

from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware

from app.consumer import KafkaDeliveryConsumer
from app.websocket import DeliveryConnectionManager

logger = logging.getLogger(__name__)

delivery_manager = DeliveryConnectionManager()
enable_kafka = os.getenv("ENABLE_KAFKA_DELIVERY", "false").lower() in ("true", "1", "yes")
kafka_consumer: Optional[KafkaDeliveryConsumer] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and graceful shutdown lifecycle."""
    global kafka_consumer
    if enable_kafka:
        kafka_consumer = KafkaDeliveryConsumer(manager=delivery_manager)
        await kafka_consumer.start()
    yield
    if kafka_consumer:
        await kafka_consumer.stop()


app = FastAPI(
    title="Conversa AI - Delivery Service",
    version="0.1.0",
    description="Real-time WebSocket transcript delivery service with session isolation",
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
    return {"service": "conversa-delivery-service", "status": "alive"}


@app.get("/health/ready", tags=["Health"])
async def readiness_probe() -> dict:
    return {
        "service": "conversa-delivery-service",
        "status": "ready",
        "active_sessions": len(delivery_manager._connections),
    }


@app.get("/stats", tags=["Observability"])
async def get_stats() -> dict:
    return delivery_manager.get_stats()


@app.websocket("/ws/delivery/{session_id}")
async def delivery_websocket_endpoint(
    websocket: WebSocket, session_id: str
) -> None:
    """Client WebSocket endpoint to receive real-time transcript updates for a session."""
    try:
        UUID(session_id)
    except ValueError:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Invalid session_id UUID"
        )
        return

    await websocket.accept()
    await delivery_manager.connect(session_id, websocket)

    # Handshake confirmation
    await websocket.send_text(
        json.dumps(
            {
                "type": "connected",
                "session_id": session_id,
                "message": "Subscribed to transcript delivery stream",
            }
        )
    )

    try:
        while True:
            # Keep-alive loop
            message = await websocket.receive_text()
            try:
                data = json.loads(message)
                if data.get("type") == "ping":
                    await websocket.send_text(
                        json.dumps({"type": "pong", "session_id": session_id})
                    )
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("Delivery WebSocket exception for %s: %s", session_id, exc)
    finally:
        await delivery_manager.disconnect(session_id, websocket)


@app.post("/api/delivery/{session_id}/events", tags=["Delivery"])
async def dispatch_event(
    session_id: str, payload: Dict[str, Any] = Body(...)
) -> dict:
    """Ingest partial, final, or error event and route to connected clients for this session."""
    try:
        UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id UUID")

    delivered_count = await delivery_manager.broadcast_to_session(session_id, payload)
    return {
        "session_id": session_id,
        "delivered_clients": delivered_count,
        "event_type": payload.get("type", "unknown"),
    }
