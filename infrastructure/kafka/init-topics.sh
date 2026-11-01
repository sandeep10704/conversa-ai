#!/usr/bin/env bash
# Conversa AI - Kafka Topic Initialization Script
# Creates all required topics with partitions and retention settings

set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION:-1}"

echo "Waiting for Kafka broker at ${BOOTSTRAP_SERVER}..."
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null 2>&1; do
    echo "Broker not ready yet. Retrying in 2 seconds..."
    sleep 2
done

echo "Kafka broker is ready. Initializing Conversa AI topics..."

TOPICS=(
    "conversation.transcript.final"
    "conversation.transcript.partial"
    "conversation.speech.activity"
    "agent.commands"
    "agent.suggestions"
    "agent.session.events"
    "agent.feedback"
    "agent.dlq"
)


for topic in "${TOPICS[@]}"; do
    echo "Creating topic: ${topic} (partitions=${PARTITIONS}, replication=${REPLICATION})..."
    /opt/kafka/bin/kafka-topics.sh --create \
        --if-not-exists \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        --topic "${topic}"
done

echo "Listing all created topics:"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "${BOOTSTRAP_SERVER}"

echo "All Conversa AI Kafka topics successfully initialized!"
