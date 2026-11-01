from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse

from app.api.conversation import router as conversation_router
from app.api.sessions import router as sessions_router
from app.api.suggestions import router as suggestions_router
from app.database.engine import dispose_engine, get_async_engine
from app.database.session import check_db_health

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan managing database engine and connection pool."""
    logger.info("Starting Conversa AI Agent Server...")
    # Initialize engine pool on startup
    _ = get_async_engine()
    yield
    # Gracefully dispose of connections on shutdown
    logger.info("Shutting down Conversa AI Agent Server...")
    await dispose_engine()


from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Conversa AI Agent Server",
    description="Independent conversation intelligence service",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    sessions_router,
    prefix="/v1"
)

app.include_router(
    conversation_router,
    prefix="/v1"
)

app.include_router(
    suggestions_router,
    prefix="/v1"
)


@app.get("/", tags=["Health"])
async def root():
    return {
        "project": "Conversa AI",
        "service": "Independent Agent Server",
        "status": "running",
        "version": "0.1.0",
        "docs": "/docs"
    }


@app.get("/health/live", tags=["Health"])
async def liveness_check():
    """Application liveness check - independent of database availability."""
    return {
        "service": "conversa-agent-server",
        "status": "alive"
    }


@app.get("/health/ready", tags=["Health"])
async def readiness_check():
    """Database readiness probe checking connection health."""
    is_healthy, db_status = await check_db_health()
    if is_healthy:
        return {
            "service": "conversa-agent-server",
            "status": "ready",
            "database": db_status
        }

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "service": "conversa-agent-server",
            "status": "not_ready",
            "database": "disconnected",
            "error": db_status
        }
    )


@app.get("/health/metrics", tags=["Health"])
async def metrics_endpoint():
    """Observability endpoint reporting outbox, event streaming, and agent execution statistics."""
    from app.database.models import AgentExecutionModel, ProcessedEventModel, SuggestionModel
    from app.database.repository import OutboxRepository
    from app.database.session import async_session_factory
    from sqlalchemy import func, select

    async with async_session_factory()() as session:
        repo = OutboxRepository(session)
        outbox_counts = await repo.get_counts()

        proc_stmt = select(func.count(ProcessedEventModel.event_id))
        proc_res = await session.execute(proc_stmt)
        processed_count = proc_res.scalar_one()

        exec_stmt = select(
            AgentExecutionModel.execution_status,
            func.count(AgentExecutionModel.execution_id),
        ).group_by(AgentExecutionModel.execution_status)
        exec_res = await session.execute(exec_stmt)
        execution_counts = dict(exec_res.all())

        sugg_stmt = select(func.count(SuggestionModel.suggestion_id))
        sugg_res = await session.execute(sugg_stmt)
        suggestion_count = sugg_res.scalar_one()

    return {
        "outbox": outbox_counts,
        "processed_events": processed_count,
        "agent_executions": {
            "running": execution_counts.get("running", 0),
            "completed": execution_counts.get("completed", 0),
            "no_suggestion": execution_counts.get("no_suggestion", 0),
            "failed": execution_counts.get("failed", 0),
            "total": sum(execution_counts.values()),
        },
        "suggestions": {
            "total": suggestion_count,
        },
    }
