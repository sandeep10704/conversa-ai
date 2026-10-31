
from fastapi import FastAPI

from app.api.sessions import router as sessions_router


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
