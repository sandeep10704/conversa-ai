from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from app.core.session_store import session_store

from app.core.conversation_store import conversation_store

from app.schemas.conversation import (
    TranscriptMessageRequest,
    TranscriptMessageResponse
)


router = APIRouter(
    prefix="/sessions",
    tags=["Conversation"]
)


@router.post(
    "/{session_id}/messages",
    response_model=TranscriptMessageResponse,
    status_code=status.HTTP_201_CREATED
)
async def add_message(
    session_id: UUID,
    request: TranscriptMessageRequest
):

    session = session_store.get(session_id)

    if session is None:

        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )

    return conversation_store.add_message(
        session_id,
        request
    )


@router.get(
    "/{session_id}/messages",
    response_model=list[TranscriptMessageResponse]
)
async def get_messages(session_id: UUID):

    session = session_store.get(session_id)

    if session is None:

        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )

    return conversation_store.get_messages(session_id)