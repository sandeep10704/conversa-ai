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


app = FastAPI(
    title="Conversa AI Agent Server",
    description="Independent conversation intelligence service",
    version="0.1.0",
    lifespan=lifespan,
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
