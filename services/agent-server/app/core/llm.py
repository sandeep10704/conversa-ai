from functools import lru_cache

from langchain_openrouter import ChatOpenRouter

from app.core.config import get_settings


class LLMConfigurationError(RuntimeError):
    """Raised when the LLM configuration is invalid."""


@lru_cache(maxsize=1)
def get_llm() -> ChatOpenRouter:
    settings = get_settings()

    if not settings.openrouter_api_key:
        raise LLMConfigurationError(
            "OPENROUTER_API_KEY is missing. Check your root .env file."
        )

    return ChatOpenRouter(
        model=settings.openrouter_model,
        api_key=settings.openrouter_api_key.get_secret_value(),
        temperature=0.1,
        max_tokens=1200,
        timeout=45_000,
        max_retries=1,
    )