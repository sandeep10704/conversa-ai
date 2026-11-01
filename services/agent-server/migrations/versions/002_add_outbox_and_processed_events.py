"""add outbox_events and processed_events tables

Revision ID: 002_outbox
Revises: 001_initial
Create Date: 2026-10-31 18:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002_outbox'
down_revision: Union[str, Sequence[str], None] = '001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create outbox_events table
    op.create_table(
        'outbox_events',
        sa.Column('event_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('aggregate_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('event_version', sa.String(length=20), nullable=False, server_default='v1'),
        sa.Column('payload', sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), 'postgresql'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('publish_attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('status', sa.String(length=30), nullable=False, server_default='pending'),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('event_id')
    )
    op.create_index('ix_outbox_events_aggregate_id', 'outbox_events', ['aggregate_id'], unique=False)
    op.create_index('ix_outbox_events_event_type', 'outbox_events', ['event_type'], unique=False)
    op.create_index('ix_outbox_events_status', 'outbox_events', ['status'], unique=False)
    op.create_index('ix_outbox_events_created_at', 'outbox_events', ['created_at'], unique=False)
    op.create_index('ix_outbox_pending_fifo', 'outbox_events', ['status', 'created_at'], unique=False)
    op.create_index('ix_outbox_aggregate', 'outbox_events', ['aggregate_id', 'created_at'], unique=False)

    # 2. Create processed_events table
    op.create_table(
        'processed_events',
        sa.Column('event_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('session_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('event_id')
    )
    op.create_index('ix_processed_events_session_id', 'processed_events', ['session_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_processed_events_session_id', table_name='processed_events')
    op.drop_table('processed_events')

    op.drop_index('ix_outbox_aggregate', table_name='outbox_events')
    op.drop_index('ix_outbox_pending_fifo', table_name='outbox_events')
    op.drop_index('ix_outbox_events_created_at', table_name='outbox_events')
    op.drop_index('ix_outbox_events_status', table_name='outbox_events')
    op.drop_index('ix_outbox_events_event_type', table_name='outbox_events')
    op.drop_index('ix_outbox_events_aggregate_id', table_name='outbox_events')
    op.drop_table('outbox_events')
