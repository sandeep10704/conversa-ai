from datetime import datetime, timezone
from threading import Lock
from uuid import UUID

from app.schemas.conversation import (
    TranscriptMessageRequest,
    TranscriptMessageResponse
)


class InMemoryConversationStore:

    def __init__(self):

        self._messages = {}
        self._lock = Lock()

    def add_message(
        self,
        session_id: UUID,
        request: TranscriptMessageRequest
    ):

        with self._lock:

            messages = self._messages.setdefault(
                session_id,
                []
            )

            # Avoid processing duplicate events
            for message in messages:

                if message.event_id == request.event_id:
                    return message

            sequence_number = len(messages) + 1

            response = TranscriptMessageResponse(
                event_id=request.event_id,
                session_id=session_id,
                sequence_number=sequence_number,
                text=request.text,
                source=request.source,
                created_at=datetime.now(timezone.utc)
            )

            messages.append(response)

            return response

    def get_messages(self, session_id: UUID):

        with self._lock:
            return list(self._messages.get(session_id, []))


conversation_store = InMemoryConversationStore()