import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from app.agent.graph import get_agent_graph
from app.core.conversation_store import get_conversation_repository
from app.core.llm import LLMConfigurationError
from app.core.parsing import StructuredOutputError
from app.core.session_store import get_session_repository
from app.database.repository import (
    ConversationRepository,
    SessionNotFoundError,
    SessionRepository,
)
from app.schemas.agent import AgentRunResponse, SuggestionItem

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sessions", tags=["Agent"])


@router.post("/{session_id}/suggest", response_model=AgentRunResponse)
async def generate_suggestions(
    session_id: UUID,
    session_repo: SessionRepository = Depends(get_session_repository),
    conv_repo: ConversationRepository = Depends(get_conversation_repository),
):

    # 1. Retrieve session from PostgreSQL
    session = await session_repo.get(session_id)

    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    # 2. Retrieve conversation history from PostgreSQL
    try:
        messages = await conv_repo.get_messages(session_id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail="Session not found")

    if not messages:
        raise HTTPException(
            status_code=400, detail="No conversation messages available"
        )


    # 3. Construct initial agent state
    initial_state = {
        "session_id": str(session_id),
        "situation": session.situation,
        "objective": session.objective,
        "communication_goals": session.communication_goals,
        "response_preferences": session.response_preferences.model_dump(),
        "constraints": session.constraints,
        "messages": [
            message.model_dump(mode="json")
            for message in messages[-20:]
        ],
        "suggestions": [],
        "validation_warnings": [],
    }

    # 4. Execute LangGraph
    try:
        graph = get_agent_graph()
        result = await graph.ainvoke(initial_state)

    except LLMConfigurationError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error

    except StructuredOutputError as error:
        logger.error(f"Agent graph structured output error at stage {error.stage}: {error}")
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Structured output error",
                "stage": error.stage,
                "category": error.category.value,
                "error": str(error),
                "validation_errors": error.validation_errors,
            },
        ) from error

    except Exception as error:
        err_msg = str(error)
        category = "api_error"
        if "rate limit" in err_msg.lower() or "429" in err_msg or "TooManyRequests" in type(error).__name__:
            category = "rate_limit"
        elif "timeout" in err_msg.lower() or "Timeout" in type(error).__name__:
            category = "timeout"

        logger.exception("Agent graph execution failed")
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Agent execution failed",
                "stage": getattr(error, "stage", "unknown"),
                "category": category,
                "error": err_msg,
            },
        ) from error

    # 5. Build API response
    analysis = result.get("analysis", {})
    relevance = result.get("relevance", {})

    suggestions = [
        SuggestionItem.model_validate(item)
        for item in result.get("suggestions", [])
    ]

    return AgentRunResponse(
        session_id=str(session_id),
        status=result.get("status", "no_suggestion"),
        current_topic=result.get("current_topic", "Unknown"),
        conversation_summary=result.get("conversation_summary", ""),
        detected_intent=result.get("detected_intent", "unclear"),
        relevance_confidence=relevance.get("confidence", 0.0),
        relevance_reason=relevance.get("reason", "Insufficient context"),
        response_opportunity=relevance.get("response_opportunity", ""),
        selected_model=result.get("selected_model"),
        stage_reached=result.get("stage_reached", "completed"),
        should_suggest=result.get("should_suggest"),
        suggestions=suggestions,
        validation_warnings=result.get("validation_warnings", []),
    )