import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.config import get_settings
from app.core.conversation_store import get_conversation_repository
from app.core.kafka_producer import AIOKafkaEventProducer
from app.database.repository import (
    ConversationRepository,
    SessionNotFoundError,
)
from app.events.speech_activity import SpeechActivityEvent, SpeechActivityPayload
from app.events.topics import KafkaTopics
from app.schemas.conversation import (
    SpeechActivityRequest,
    TranscriptMessageRequest,
    TranscriptMessageResponse,
)

logger = logging.getLogger(__name__)

_speech_producer: Optional[AIOKafkaEventProducer] = None


async def get_speech_activity_producer() -> Optional[AIOKafkaEventProducer]:
    global _speech_producer
    if _speech_producer is None:
        settings = get_settings()
        try:
            _speech_producer = AIOKafkaEventProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id="conversa-speech-activity-api-producer",
            )
            await _speech_producer.start()
        except Exception as exc:
            logger.warning("Could not initialize Kafka producer for speech activity: %s", exc)
    return _speech_producer

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


@router.post(
    "/{session_id}/speech-activity",
    status_code=status.HTTP_202_ACCEPTED,
)
async def record_speech_activity(
    session_id: UUID,
    request: SpeechActivityRequest,
    producer: Optional[AIOKafkaEventProducer] = Depends(get_speech_activity_producer),
):
    """Receive real-time speech onset notifications and publish directly to Kafka."""
    event = SpeechActivityEvent(
        event_id=request.event_id,
        session_id=session_id,
        payload=SpeechActivityPayload(
            activity_type=request.activity_type,
            activity_sequence=request.activity_sequence,
            timestamp=request.timestamp,
            source_sequence=request.source_sequence,
            last_finalized_sequence=request.last_finalized_sequence,
        ),

    )
    if producer is not None:
        try:
            await producer.publish(
                topic=KafkaTopics.SPEECH_ACTIVITY.value,
                key=str(session_id),
                value=event.model_dump(mode="json"),
                event_id=str(event.event_id),
            )
        except Exception as exc:
            logger.warning("[Speech Activity API] Failed to publish event: %s", exc)

    return {"status": "accepted", "event_id": str(event.event_id)}
