"""Main entrypoint for independent Agent Trigger Controller process."""

import asyncio
import logging
import signal

from app.core.config import get_settings
from app.orchestration.trigger_controller import AgentTriggerController

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TriggerController] %(message)s",
)
logger = logging.getLogger("trigger_controller_main")


async def main() -> None:
    settings = get_settings()
    logger.info("Initializing Agent Trigger Controller process...")
    logger.info(f"Kafka bootstrap servers: {settings.kafka_bootstrap_servers}")
    logger.info(f"AGENT_AUTO_RUN: {settings.agent_auto_run}")
    logger.info(f"Min new messages: {settings.agent_trigger_min_new_messages}")
    logger.info(f"Debounce: {settings.agent_trigger_debounce_ms} ms")
    logger.info(f"Cooldown: {settings.agent_session_cooldown_seconds} s")

    controller = AgentTriggerController()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Shutdown signal received, stopping trigger controller...")
        asyncio.create_task(controller.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        await controller.run_loop()
    except Exception as exc:
        logger.critical(f"Trigger Controller terminated with unhandled error: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
