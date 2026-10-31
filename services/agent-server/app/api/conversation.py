from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.conversation_store import get_conversation_repository
from app.database.repository import (
    ConversationRepository,
    SessionNotFoundError,
)
from app.schemas.conversation import (
    TranscriptMessageRequest,
    TranscriptMessageResponse,
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
    request: TranscriptMessageRequest,
    conv_repo: ConversationRepository = Depends(get_conversation_repository),
):
    try:
        return await conv_repo.add_message(session_id, request)
    except SessionNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )


@router.get(
    "/{session_id}/messages",
    response_model=list[TranscriptMessageResponse]
)
async def get_messages(
    session_id: UUID,
    conv_repo: ConversationRepository = Depends(get_conversation_repository),
):
    try:
        return await conv_repo.get_messages(session_id)
    except SessionNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )