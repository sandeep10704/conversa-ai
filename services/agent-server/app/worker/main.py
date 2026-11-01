"""Main entrypoint for independent Agent Worker process."""

import asyncio
import logging
import signal

from app.core.config import get_settings
from app.worker.worker import AgentWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [AgentWorker] %(message)s",
)
logger = logging.getLogger("agent_worker_main")


async def main() -> None:
    settings = get_settings()
    logger.info("Initializing Agent Worker process...")
    logger.info(f"Kafka bootstrap servers: {settings.kafka_bootstrap_servers}")
    logger.info(f"MOCK_LLM: {settings.mock_llm}")
    logger.info(f"Model: {settings.openrouter_model}")

    worker = AgentWorker()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Shutdown signal received, stopping agent worker...")
        asyncio.create_task(worker.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        await worker.run_loop()
    except Exception as exc:
        logger.critical(f"Agent Worker terminated with unhandled error: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
