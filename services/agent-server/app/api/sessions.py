
from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from app.schemas.session import (
    CreateSessionRequest,
    SessionResponse
)

from app.core.session_store import session_store


router = APIRouter(
    prefix="/sessions",
    tags=["Sessions"]
)


@router.post(
    "",
    response_model=SessionResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_session(request: CreateSessionRequest):

    return session_store.create(request)


@router.get(
    "/{session_id}",
    response_model=SessionResponse
)
async def get_session(session_id: UUID):

    session = session_store.get(session_id)

    if session is None:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )

    return session
