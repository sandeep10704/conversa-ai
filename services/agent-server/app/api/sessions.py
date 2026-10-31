from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.session_store import get_session_repository
from app.database.repository import SessionRepository
from app.schemas.session import (
    CreateSessionRequest,
    SessionResponse,
)

router = APIRouter(
    prefix="/sessions",
    tags=["Sessions"]
)


@router.post(
    "",
    response_model=SessionResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_session(
    request: CreateSessionRequest,
    repo: SessionRepository = Depends(get_session_repository),
):
    return await repo.create(request)


@router.get(
    "/{session_id}",
    response_model=SessionResponse
)
async def get_session(
    session_id: UUID,
    repo: SessionRepository = Depends(get_session_repository),
):
    session = await repo.get(session_id)

    if session is None:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )

    return session


@router.delete(
    "/{session_id}",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_session(
    session_id: UUID,
    repo: SessionRepository = Depends(get_session_repository),
):
    deleted = await repo.delete(session_id)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )

    return None

