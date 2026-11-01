"""Versioned event schemas for Conversa AI event-driven architecture."""

from app.events.agent import (
    FeedbackEvent,
    FeedbackPayload,
    SessionLifecycleEvent,
    SessionLifecyclePayload,
    SuggestionEvent,
    SuggestionGeneratedEvent,
    SuggestionGeneratedPayload,
    SuggestionItemPayload,
    SuggestionPayload,
)
from app.events.base import BaseEvent, EventCompatibility, utc_now
from app.events.commands import AgentCommandEvent, AgentCommandPayload
from app.events.dlq import DLQEvent, DLQPayload
from app.events.topics import KafkaTopics
from app.events.transcript import (
    TranscriptFinalEvent,
    TranscriptFinalPayload,
    TranscriptPartialEvent,
    TranscriptPartialPayload,
)

__all__ = [
    "KafkaTopics",
    "BaseEvent",
    "EventCompatibility",
    "utc_now",
    "TranscriptFinalPayload",
    "TranscriptFinalEvent",
    "TranscriptPartialPayload",
    "TranscriptPartialEvent",
    "AgentCommandPayload",
    "AgentCommandEvent",
    "SessionLifecyclePayload",
    "SessionLifecycleEvent",
    "SuggestionItemPayload",
    "SuggestionPayload",
    "SuggestionEvent",
    "SuggestionGeneratedPayload",
    "SuggestionGeneratedEvent",
    "FeedbackPayload",
    "FeedbackEvent",
    "DLQPayload",
    "DLQEvent",
]
