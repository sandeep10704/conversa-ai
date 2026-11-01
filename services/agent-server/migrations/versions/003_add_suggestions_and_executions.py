"""add agent_executions and suggestions tables, update processed_events PK

Revision ID: 003_orchestration
Revises: 002_outbox
Create Date: 2026-10-31 20:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '003_orchestration'
down_revision: Union[str, Sequence[str], None] = '002_outbox'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Update processed_events with consumer_id if table exists
    conn = op.get_bind()
    # Add consumer_id column if not exists
    op.add_column(
        'processed_events',
        sa.Column('consumer_id', sa.String(length=100), nullable=False, server_default='default')
    )

    # 2. Create agent_executions table
    op.create_table(
        'agent_executions',
        sa.Column('execution_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('session_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('trigger_reason', sa.String(length=100), nullable=False, server_default='auto_trigger'),
        sa.Column('processed_sequence_number', sa.Integer(), nullable=False),
        sa.Column('selected_model', sa.String(length=100), nullable=True),
        sa.Column('execution_status', sa.String(length=50), nullable=False, server_default='pending'),
        sa.Column('failure_stage', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['session_id'], ['sessions.session_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('execution_id')
    )
    op.create_index('ix_agent_executions_session', 'agent_executions', ['session_id'], unique=False)
    op.create_index('ix_agent_executions_session_status', 'agent_executions', ['session_id', 'execution_status'], unique=False)
    op.create_index('ix_agent_executions_session_seq', 'agent_executions', ['session_id', 'processed_sequence_number'], unique=False)

    # 3. Create suggestions table
    op.create_table(
        'suggestions',
        sa.Column('suggestion_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('session_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('execution_id', sa.Uuid(as_uuid=True), nullable=True),
        sa.Column('source_last_sequence_number', sa.Integer(), nullable=False),
        sa.Column('response_type', sa.String(length=50), nullable=False),
        sa.Column('suggestion_text', sa.Text(), nullable=False),
        sa.Column('rationale', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('delivery_status', sa.String(length=50), nullable=False, server_default='pending'),
        sa.ForeignKeyConstraint(['session_id'], ['sessions.session_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['execution_id'], ['agent_executions.execution_id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('suggestion_id')
    )
    op.create_index('ix_suggestions_session', 'suggestions', ['session_id'], unique=False)
    op.create_index('ix_suggestions_session_created', 'suggestions', ['session_id', 'created_at'], unique=False)
    op.create_index('ix_suggestions_execution', 'suggestions', ['execution_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_suggestions_execution', table_name='suggestions')
    op.drop_index('ix_suggestions_session_created', table_name='suggestions')
    op.drop_index('ix_suggestions_session', table_name='suggestions')
    op.drop_table('suggestions')

    op.drop_index('ix_agent_executions_session_seq', table_name='agent_executions')
    op.drop_index('ix_agent_executions_session_status', table_name='agent_executions')
    op.drop_index('ix_agent_executions_session', table_name='agent_executions')
    op.drop_table('agent_executions')

    op.drop_column('processed_events', 'consumer_id')
