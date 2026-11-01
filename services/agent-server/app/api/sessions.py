from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.session_store import get_session_repository
from app.database.repository import SessionRepository
from app.schemas.participant_context import ParticipantContext
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


@router.put(
    "/{session_id}/participant-context",
    response_model=SessionResponse,
    status_code=status.HTTP_200_OK,
)
async def update_participant_context(
    session_id: UUID,
    request: ParticipantContext,
    repo: SessionRepository = Depends(get_session_repository),
):
    """Update or replace participant context for an active session."""
    updated = await repo.update_participant_context(session_id, request)
    if updated is None:
        raise HTTPException(
            status_code=404,
            detail="Session not found",
        )
    return updated


@router.delete(
    "/{session_id}/participant-context",
    response_model=SessionResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_participant_context(
    session_id: UUID,
    repo: SessionRepository = Depends(get_session_repository),
):
    """Clear participant context from an active session."""
    updated = await repo.update_participant_context(session_id, None)
    if updated is None:
        raise HTTPException(
            status_code=404,
            detail="Session not found",
        )
    return updated


@router.post(
    "/{session_id}/end",
    response_model=SessionResponse,
    status_code=status.HTTP_200_OK,
)
async def end_session(
    session_id: UUID,
    repo: SessionRepository = Depends(get_session_repository),
):
    """End a session and explicitly purge participant context for privacy."""
    session = await repo.end_session(session_id)
    if session is None:
        raise HTTPException(
            status_code=404,
            detail="Session not found",
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
