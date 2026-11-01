"""Conversa AI Audio Gateway Service.

FastAPI entrypoint providing WebSocket endpoints for real-time audio chunk ingestion
and forwarding to the Transcription Service.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.websocket import gateway_manager, router as ws_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and graceful shutdown lifecycle."""
    yield
    # Disconnect all active sessions on shutdown
    active_ids = list(gateway_manager.sessions.keys())
    for sid in active_ids:
        await gateway_manager.disconnect_session(sid)


app = FastAPI(
    title="Conversa AI - Audio Gateway",
    version="0.1.0",
    description="WebSocket-based live audio ingestion and forwarding service",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ws_router)


@app.get("/health/live", tags=["Health"])
async def liveness_probe() -> dict:
    """Liveness probe verifying the service process is responsive."""
    return {"service": "conversa-audio-gateway", "status": "alive"}


@app.get("/health/ready", tags=["Health"])
async def readiness_probe() -> dict:
    """Readiness probe verifying operational status."""
    return {
        "service": "conversa-audio-gateway",
        "status": "ready",
        "active_sessions": len(gateway_manager.sessions),
    }


@app.get("/stats", tags=["Observability"])
async def get_stats() -> dict:
    """Return aggregated audio ingestion statistics."""
    return gateway_manager.get_global_stats()


@app.get("/stats/{session_id}", tags=["Observability"])
async def get_session_stats(session_id: str) -> dict:
    """Return statistics for a specific active session."""
    stats = gateway_manager.get_session_stats(session_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Session not found or inactive")
    return stats
