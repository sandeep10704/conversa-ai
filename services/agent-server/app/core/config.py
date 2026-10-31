from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def load_project_environment():

    current_file = Path(__file__).resolve()

    for parent in current_file.parents:

        project_marker = (
            parent / "services" / "agent-server" / "pyproject.toml"
        )

        if project_marker.is_file():

            env_path = parent / ".env"

            if env_path.is_file():
                load_dotenv(
                    dotenv_path=env_path,
                    override=False
                )

            return


# Local development loads the root .env.
# Docker receives environment variables through Docker Compose.
load_project_environment()


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        extra="ignore",
        case_sensitive=False
    )

    openrouter_api_key: SecretStr | None = None

    openrouter_model: str = (
        "deepseek/deepseek-v4.1-flash"
    )

    agent_host: str = "0.0.0.0"

    agent_port: int = 8001

    app_env: str = "development"

    log_level: str = "INFO"

    # PostgreSQL Database Settings
    postgres_user: str = "conversa_user"
    postgres_password: str = "conversa_password"
    postgres_db: str = "conversa"
    database_url: str = (
        "postgresql+asyncpg://conversa_user:conversa_password@localhost:5432/conversa"
    )
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_timeout: int = 30
    database_pool_recycle: int = 1800

    @property
    def async_database_url(self) -> str:
        """Ensure database URL uses asyncpg driver dialect."""
        url = self.database_url
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql+asyncpg://", 1)
        if url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url


@lru_cache
def get_settings() -> Settings:

    return Settings()