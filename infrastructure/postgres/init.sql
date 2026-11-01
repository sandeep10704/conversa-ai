-- Conversa AI PostgreSQL Initialization Schema
-- Initializes sessions and conversation_messages tables with constraints and indexes

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Sessions Table
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    situation TEXT NOT NULL,
    objective TEXT NOT NULL,
    communication_goals JSONB NOT NULL DEFAULT '[]'::jsonb,
    response_preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
    constraints JSONB NOT NULL DEFAULT '[]'::jsonb
);

-- Index on session status
CREATE INDEX IF NOT EXISTS ix_sessions_status ON sessions(status);

-- 2. Conversation Messages Table
CREATE TABLE IF NOT EXISTS conversation_messages (
    message_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL,
    text TEXT NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'manual',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_session_sequence UNIQUE (session_id, sequence_number)
);

-- Index on session_id for high-throughput message queries
CREATE INDEX IF NOT EXISTS ix_messages_session_id ON conversation_messages(session_id);
CREATE INDEX IF NOT EXISTS ix_messages_session_seq ON conversation_messages(session_id, sequence_number);

-- 3. Transactional Outbox Events Table
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_version VARCHAR(20) NOT NULL DEFAULT 'v1',
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    last_error TEXT
);

-- Indexes for efficient FIFO outbox polling and aggregate tracking
CREATE INDEX IF NOT EXISTS ix_outbox_pending_fifo ON outbox_events(status, created_at);
CREATE INDEX IF NOT EXISTS ix_outbox_aggregate ON outbox_events(aggregate_id, created_at);

-- 4. Processed Events Table (Consumer Idempotency Tracking with independent consumer identities)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID NOT NULL,
    consumer_id VARCHAR(100) NOT NULL DEFAULT 'default',
    event_type VARCHAR(100) NOT NULL,
    session_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, consumer_id)
);

CREATE INDEX IF NOT EXISTS ix_processed_events_session ON processed_events(session_id);

-- 5. Agent Executions Table (Orchestration & Run Telemetry)
CREATE TABLE IF NOT EXISTS agent_executions (
    execution_id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    trigger_reason VARCHAR(100) NOT NULL DEFAULT 'auto_trigger',
    processed_sequence_number INTEGER NOT NULL,
    selected_model VARCHAR(100),
    execution_status VARCHAR(50) NOT NULL DEFAULT 'pending',
    failure_stage VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_agent_executions_session_status ON agent_executions(session_id, execution_status);
CREATE INDEX IF NOT EXISTS ix_agent_executions_session_seq ON agent_executions(session_id, processed_sequence_number);

-- 6. Suggestions Table (Persisted Suggestions & Delivery Tracking)
CREATE TABLE IF NOT EXISTS suggestions (
    suggestion_id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    execution_id UUID REFERENCES agent_executions(execution_id) ON DELETE SET NULL,
    source_last_sequence_number INTEGER NOT NULL,
    response_type VARCHAR(50) NOT NULL,
    suggestion_text TEXT NOT NULL,
    rationale TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivery_status VARCHAR(50) NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS ix_suggestions_session_created ON suggestions(session_id, created_at);
CREATE INDEX IF NOT EXISTS ix_suggestions_execution ON suggestions(execution_id);
