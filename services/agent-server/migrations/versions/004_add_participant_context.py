"""add participant_context column to sessions table

Revision ID: 004_participant_context
Revises: 003_orchestration
Create Date: 2026-11-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '004_participant_context'
down_revision: Union[str, Sequence[str], None] = '003_orchestration'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Compatible JSON type: native JSONB on PostgreSQL, standard JSON elsewhere
    conn = op.get_bind()
    dialect_name = conn.dialect.name if conn else ""

    if dialect_name == "postgresql":
        json_type = postgresql.JSONB(astext_type=sa.Text())
    else:
        json_type = sa.JSON()

    op.add_column(
        'sessions',
        sa.Column('participant_context', json_type, nullable=True)
    )


def downgrade() -> None:
    op.drop_column('sessions', 'participant_context')
