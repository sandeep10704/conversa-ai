
from fastapi import FastAPI

from app.api.sessions import router as sessions_router
from app.api.conversation import router as conversation_router

app = FastAPI(
    title="Conversa AI Agent Server",
    description="Independent conversation intelligence service",
    version="0.1.0"
)

app.include_router(
    sessions_router,
    prefix="/v1"
)


@app.get("/health/live", tags=["Health"])
async def health_check():

    return {
        "service": "conversa-agent-server",
        "status": "alive"
    }

@app.get("/", tags=["Health"])
async def root():

    return {
        "project": "Conversa AI",
        "service": "Independent Agent Server",
        "status": "running",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/health/live"
    }

app.include_router(
    conversation_router,
    prefix="/v1"
)