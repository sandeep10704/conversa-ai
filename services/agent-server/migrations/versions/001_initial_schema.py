"""create sessions and conversation_messages tables

Revision ID: 001_initial
Revises:
Create Date: 2026-10-31 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create sessions table
    op.create_table(
        'sessions',
        sa.Column('session_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='active'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('situation', sa.Text(), nullable=False),
        sa.Column('objective', sa.Text(), nullable=False),
        sa.Column('communication_goals', sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), 'postgresql'), nullable=False),
        sa.Column('response_preferences', sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), 'postgresql'), nullable=False),
        sa.Column('constraints', sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), 'postgresql'), nullable=False),
        sa.PrimaryKeyConstraint('session_id')
    )

    # 2. Create conversation_messages table
    op.create_table(
        'conversation_messages',
        sa.Column('message_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('session_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('sequence_number', sa.Integer(), nullable=False),
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('source', sa.String(length=50), nullable=False, server_default='manual'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['session_id'], ['sessions.session_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('message_id'),
        sa.UniqueConstraint('session_id', 'sequence_number', name='uq_session_sequence')
    )

    # Indexes
    op.create_index('ix_messages_session_id', 'conversation_messages', ['session_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_messages_session_id', table_name='conversation_messages')
    op.drop_table('conversation_messages')
    op.drop_table('sessions')
