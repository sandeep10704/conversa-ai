"""Kafka topic definitions for Conversa AI."""

from enum import Enum


class KafkaTopics(str, Enum):
    """Core Kafka topics for Conversa AI event streaming."""

    TRANSCRIPT_FINAL = "conversation.transcript.final"
    TRANSCRIPT_PARTIAL = "conversation.transcript.partial"
    AGENT_COMMANDS = "agent.commands"
    AGENT_SUGGESTIONS = "agent.suggestions"
    SESSION_EVENTS = "agent.session.events"
    AGENT_FEEDBACK = "agent.feedback"
    AGENT_DLQ = "agent.dlq"
