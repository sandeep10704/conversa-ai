"""Main entrypoint for independent Kafka Consumer Worker process."""

import asyncio
import logging
import signal

from app.consumer.worker import TranscriptConsumerWorker
from app.core.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ConsumerWorker] %(message)s",
)
logger = logging.getLogger("consumer_worker_main")


async def main() -> None:
    settings = get_settings()
    logger.info("Initializing Transcript Consumer Worker process...")
    logger.info(f"Kafka bootstrap servers: {settings.kafka_bootstrap_servers}")
    logger.info(f"Consumer group: {settings.consumer_group_id}")
    logger.info(f"AGENT_AUTO_RUN: {settings.agent_auto_run}")

    worker = TranscriptConsumerWorker()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Shutdown signal received, stopping consumer worker...")
        asyncio.create_task(worker.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        await worker.run_loop()
    except Exception as exc:
        logger.critical(f"Consumer Worker terminated with unhandled error: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
