
from datetime import datetime, timezone
from uuid import UUID, uuid4

from app.schemas.session import (
    CreateSessionRequest,
    SessionResponse
)


class InMemorySessionStore:

    def __init__(self):
        self._sessions: dict[UUID, SessionResponse] = {}

    def create(self, request: CreateSessionRequest):
        session = SessionResponse(
            session_id=uuid4(),
            status="active",
            created_at=datetime.now(timezone.utc),
            **request.model_dump()
        )

        self._sessions[session.session_id] = session

        return session

    def get(self, session_id: UUID):
        return self._sessions.get(session_id)


session_store = InMemorySessionStore()
