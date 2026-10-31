"""Comprehensive persistence tests for Conversa AI PostgreSQL layer.

Tests:
1. Creating a session.
2. Retrieving a session.
3. Inserting messages.
4. Retrieving messages chronologically.
5. Rejecting messages for nonexistent sessions.
6. Persisting sessions across container/engine restarts.
7. Persisting messages across container/engine restarts.
8. Deleting a session with foreign-key cascade cleanup.
9. FastAPI API endpoints with database dependency injection.

Does NOT invoke OpenRouter or LangGraph.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import tempfile
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.database.models import (
    Base,
    ConversationMessageModel,
    SessionModel,
)
from app.database.repository import (
    ConversationRepository,
    SessionNotFoundError,
    SessionRepository,
)
from app.database.session import get_db_session
from app.main import app
from app.schemas.conversation import TranscriptMessageRequest
from app.schemas.session import (
    CreateSessionRequest,
    ResponsePreferences,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def temp_db_engine():
    """Create a temporary file-based SQLite database for isolated restart tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_persistence.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"

        engine = create_async_engine(db_url, echo=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        yield engine, db_url

        await engine.dispose()


@pytest.fixture
async def db_session(temp_db_engine):
    """Provide a transactional AsyncSession bound to the test database."""
    engine, _ = temp_db_engine
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with session_factory() as session:
        yield session


class TestDatabasePersistence:
    """Test suite covering the 7 core persistence requirements."""

    async def test_01_create_session(self, db_session: AsyncSession):
        """1. Test creating a session in the database."""
        repo = SessionRepository(db_session)
        request = CreateSessionRequest(
            situation="Team debugging microservices incident",
            objective="Clarify connection pool exhaustion",
            communication_goals=["Explain pool exhaustion", "Suggest pool size increase"],
            response_preferences=ResponsePreferences(
                style="technical",
                length="concise",
                technical_level="advanced",
            ),
            constraints=["Do not assign blame", "Do not mention credentials"],
        )

        session = await repo.create(request)
        await db_session.commit()

        assert session is not None
        assert isinstance(session.session_id, UUID)
        assert session.status == "active"
        assert session.situation == "Team debugging microservices incident"
        assert session.objective == "Clarify connection pool exhaustion"
        assert len(session.communication_goals) == 2
        assert session.response_preferences.style == "technical"
        assert len(session.constraints) == 2
        assert isinstance(session.created_at, datetime)

    async def test_02_retrieve_session(self, db_session: AsyncSession):
        """2. Test retrieving a session by UUID."""
        repo = SessionRepository(db_session)
        request = CreateSessionRequest(
            situation="Standup meeting discussion",
            objective="Share update on payment service latency",
            communication_goals=["Update status"],
            response_preferences=ResponsePreferences(),
            constraints=[],
        )

        created = await repo.create(request)
        await db_session.commit()

        retrieved = await repo.get(created.session_id)
        assert retrieved is not None
        assert retrieved.session_id == created.session_id
        assert retrieved.situation == created.situation
        assert retrieved.objective == created.objective

        # Verify nonexistent session returns None
        non_existent = await repo.get(uuid4())
        assert non_existent is None

    async def test_03_insert_messages(self, db_session: AsyncSession):
        """3. Test inserting conversation messages into a session."""
        session_repo = SessionRepository(db_session)
        conv_repo = ConversationRepository(db_session)

        session = await session_repo.create(
            CreateSessionRequest(
                situation="Architecture review",
                objective="Discuss Kafka topic partitioning",
            )
        )
        await db_session.commit()

        msg_req = TranscriptMessageRequest(
            text="How many partitions should we allocate for user-events?",
            source="manual",
        )
        message = await conv_repo.add_message(session.session_id, msg_req)
        await db_session.commit()

        assert message is not None
        assert message.session_id == session.session_id
        assert message.sequence_number == 1
        assert message.text == "How many partitions should we allocate for user-events?"
        assert message.source == "manual"
        assert message.event_id == msg_req.event_id

    async def test_04_retrieve_messages_chronologically(self, db_session: AsyncSession):
        """4. Test retrieving messages in strict chronological order by sequence_number."""
        session_repo = SessionRepository(db_session)
        conv_repo = ConversationRepository(db_session)

        session = await session_repo.create(
            CreateSessionRequest(
                situation="Code review session",
                objective="Review pull request #102",
            )
        )
        await db_session.commit()

        texts = [
            "First message: checking pull request",
            "Second message: reviewing the database migration",
            "Third message: looks good, approving now",
        ]

        for text_content in texts:
            await conv_repo.add_message(
                session.session_id,
                TranscriptMessageRequest(text=text_content),
            )
        await db_session.commit()

        messages = await conv_repo.get_messages(session.session_id)
        assert len(messages) == 3
        assert [m.sequence_number for m in messages] == [1, 2, 3]
        assert [m.text for m in messages] == texts

    async def test_05_reject_messages_for_nonexistent_session(self, db_session: AsyncSession):
        """5. Test that inserting a message to a nonexistent session raises SessionNotFoundError."""
        conv_repo = ConversationRepository(db_session)
        random_session_id = uuid4()

        with pytest.raises(SessionNotFoundError):
            await conv_repo.add_message(
                random_session_id,
                TranscriptMessageRequest(text="Message to nowhere"),
            )

        with pytest.raises(SessionNotFoundError):
            await conv_repo.get_messages(random_session_id)

    async def test_06_persisting_sessions_across_restarts(self, temp_db_engine):
        """6. Test that sessions persist across engine/container disposal and restart."""
        old_engine, db_url = temp_db_engine

        # Step 1: Create session with initial engine
        session_maker_1 = async_sessionmaker(old_engine, class_=AsyncSession, expire_on_commit=False)
        async with session_maker_1() as s1:
            repo1 = SessionRepository(s1)
            session = await repo1.create(
                CreateSessionRequest(
                    situation="Pre-restart test context",
                    objective="Verify persistence across server reboot",
                    communication_goals=["Survive shutdown"],
                )
            )
            await s1.commit()
            target_id = session.session_id

        # Step 2: Simulate container / server shutdown
        await old_engine.dispose()

        # Step 3: Simulate container reboot with fresh engine pointing to same storage
        new_engine = create_async_engine(db_url, echo=False)
        session_maker_2 = async_sessionmaker(new_engine, class_=AsyncSession, expire_on_commit=False)
        async with session_maker_2() as s2:
            repo2 = SessionRepository(s2)
            recovered_session = await repo2.get(target_id)

            assert recovered_session is not None
            assert recovered_session.session_id == target_id
            assert recovered_session.situation == "Pre-restart test context"
            assert recovered_session.objective == "Verify persistence across server reboot"
            assert recovered_session.communication_goals == ["Survive shutdown"]

        await new_engine.dispose()

    async def test_07_persisting_messages_across_restarts(self, temp_db_engine):
        """7. Test that conversation messages and sequences persist across container reboot."""
        old_engine, db_url = temp_db_engine

        # Step 1: Create session and multiple messages
        session_maker_1 = async_sessionmaker(old_engine, class_=AsyncSession, expire_on_commit=False)
        async with session_maker_1() as s1:
            session_repo = SessionRepository(s1)
            conv_repo = ConversationRepository(s1)

            session = await session_repo.create(
                CreateSessionRequest(
                    situation="Session with messages before restart",
                    objective="Persist message history",
                )
            )
            target_id = session.session_id

            await conv_repo.add_message(target_id, TranscriptMessageRequest(text="Message Alpha"))
            await conv_repo.add_message(target_id, TranscriptMessageRequest(text="Message Beta"))
            await conv_repo.add_message(target_id, TranscriptMessageRequest(text="Message Gamma"))
            await s1.commit()

        # Step 2: Simulate container shutdown
        await old_engine.dispose()

        # Step 3: Restart container with fresh engine
        new_engine = create_async_engine(db_url, echo=False)
        session_maker_2 = async_sessionmaker(new_engine, class_=AsyncSession, expire_on_commit=False)
        async with session_maker_2() as s2:
            conv_repo_2 = ConversationRepository(s2)
            recovered_messages = await conv_repo_2.get_messages(target_id)

            assert len(recovered_messages) == 3
            assert [m.sequence_number for m in recovered_messages] == [1, 2, 3]
            assert [m.text for m in recovered_messages] == ["Message Alpha", "Message Beta", "Message Gamma"]

            # Verify that adding a new message continues sequence monotonicity
            msg_delta = await conv_repo_2.add_message(target_id, TranscriptMessageRequest(text="Message Delta"))
            await s2.commit()
            assert msg_delta.sequence_number == 4

        await new_engine.dispose()

    async def test_08_delete_session_cascade(self, db_session: AsyncSession):
        """8. Test that deleting a session cascades and removes all associated messages."""
        session_repo = SessionRepository(db_session)
        conv_repo = ConversationRepository(db_session)

        session = await session_repo.create(
            CreateSessionRequest(situation="To be deleted", objective="Test cascade")
        )
        await conv_repo.add_message(session.session_id, TranscriptMessageRequest(text="Will be deleted"))
        await db_session.commit()

        # Delete session
        deleted = await session_repo.delete(session.session_id)
        await db_session.commit()
        assert deleted is True

        # Verify session is gone
        assert await session_repo.get(session.session_id) is None

        # Verify getting messages raises SessionNotFoundError
        with pytest.raises(SessionNotFoundError):
            await conv_repo.get_messages(session.session_id)


class TestFastAPIIntegrationWithDatabase:
    """Test FastAPI HTTP endpoints with database dependency overrides."""

    async def test_api_session_and_messages_flow(self, temp_db_engine):
        engine, _ = temp_db_engine
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async def override_get_db_session():
            async with session_factory() as session:
                try:
                    yield session
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        app.dependency_overrides[get_db_session] = override_get_db_session

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # 1. Health checks
            live_resp = await client.get("/health/live")
            assert live_resp.status_code == 200
            assert live_resp.json()["status"] == "alive"

            # 2. Create session
            create_resp = await client.post(
                "/v1/sessions",
                json={
                    "situation": "Testing FastAPI PostgreSQL endpoints",
                    "objective": "Verify HTTP 201 and JSON contracts",
                    "communication_goals": ["Goal 1"],
                    "response_preferences": {"style": "professional"},
                    "constraints": [],
                },
            )
            assert create_resp.status_code == 201
            session_data = create_resp.json()
            session_id = session_data["session_id"]
            assert session_data["status"] == "active"

            # 3. Get session
            get_resp = await client.get(f"/v1/sessions/{session_id}")
            assert get_resp.status_code == 200
            assert get_resp.json()["session_id"] == session_id

            # 4. Add messages
            msg1_resp = await client.post(
                f"/v1/sessions/{session_id}/messages",
                json={"text": "Hello world from API test", "source": "manual"},
            )
            assert msg1_resp.status_code == 201
            assert msg1_resp.json()["sequence_number"] == 1

            msg2_resp = await client.post(
                f"/v1/sessions/{session_id}/messages",
                json={"text": "Second message from API test"},
            )
            assert msg2_resp.status_code == 201
            assert msg2_resp.json()["sequence_number"] == 2

            # 5. Get messages
            msgs_resp = await client.get(f"/v1/sessions/{session_id}/messages")
            assert msgs_resp.status_code == 200
            messages = msgs_resp.json()
            assert len(messages) == 2
            assert messages[0]["sequence_number"] == 1
            assert messages[1]["sequence_number"] == 2

            # 6. Reject message for nonexistent session
            fake_id = str(uuid4())
            fake_msg_resp = await client.post(
                f"/v1/sessions/{fake_id}/messages",
                json={"text": "Ghost message"},
            )
            assert fake_msg_resp.status_code == 404

            # 7. Delete session
            del_resp = await client.delete(f"/v1/sessions/{session_id}")
            assert del_resp.status_code == 204

            # Verify 404 after deletion
            get_after_del = await client.get(f"/v1/sessions/{session_id}")
            assert get_after_del.status_code == 404

        app.dependency_overrides.clear()
