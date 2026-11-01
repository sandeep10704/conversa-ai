"""WebSocket connection manager for real-time transcript delivery.

Enforces strict per-session data isolation: clients connected to session A
can NEVER receive partial or finalized transcript events from session B.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Set
from uuid import UUID

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class DeliveryConnectionManager:
    """Manages active client WebSocket subscriptions keyed strictly by session_id."""

    def __init__(self) -> None:
        self._connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()
        self._total_messages_dispatched: int = 0

    async def connect(self, session_id: str, websocket: WebSocket) -> None:
        """Register a client WebSocket connection for a specific session."""
        async with self._lock:
            if session_id not in self._connections:
                self._connections[session_id] = set()
            self._connections[session_id].add(websocket)
            logger.info(
                "Client connected to delivery for session %s. Subscribers for session: %d",
                session_id,
                len(self._connections[session_id]),
            )

    async def disconnect(self, session_id: str, websocket: WebSocket) -> None:
        """Unregister a client WebSocket connection."""
        async with self._lock:
            subs = self._connections.get(session_id)
            if subs and websocket in subs:
                subs.remove(websocket)
                if not subs:
                    del self._connections[session_id]
                logger.info(
                    "Client disconnected from delivery for session %s. Remaining: %d",
                    session_id,
                    len(subs) if subs else 0,
                )

    async def broadcast_to_session(self, session_id: str, payload: Dict[str, Any]) -> int:
        """Broadcast payload strictly to WebSockets subscribed to this session_id.

        Returns number of clients successfully delivered to.
        """
        async with self._lock:
            clients = list(self._connections.get(session_id, []))

        if not clients:
            return 0

        # Ensure session_id matches in payload for consistency
        payload["session_id"] = session_id
        text_data = json.dumps(payload)
        delivered = 0
        dead_sockets: List[WebSocket] = []

        for ws in clients:
            try:
                await ws.send_text(text_data)
                delivered += 1
            except Exception as exc:
                logger.warning(
                    "Failed to deliver message to client in session %s: %s",
                    session_id,
                    exc,
                )
                dead_sockets.append(ws)

        if dead_sockets:
            async with self._lock:
                for dead_ws in dead_sockets:
                    if session_id in self._connections:
                        self._connections[session_id].discard(dead_ws)

        self._total_messages_dispatched += delivered
        return delivered

    def get_stats(self) -> Dict[str, Any]:
        return {
            "active_sessions_count": len(self._connections),
            "total_connected_clients": sum(
                len(s) for s in self._connections.values()
            ),
            "total_messages_dispatched": self._total_messages_dispatched,
            "sessions": {
                sid: len(clients) for sid, clients in self._connections.items()
            },
        }
