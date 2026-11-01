"""Main entrypoint for independent Outbox Publisher process."""

import asyncio
import logging
import signal

from app.core.config import get_settings
from app.publisher.service import OutboxPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [OutboxPublisher] %(message)s",
)
logger = logging.getLogger("outbox_publisher_main")


async def main() -> None:
    settings = get_settings()
    logger.info("Initializing Outbox Publisher process...")
    logger.info(f"Kafka bootstrap servers: {settings.kafka_bootstrap_servers}")

    publisher = OutboxPublisher()

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Shutdown signal received, stopping publisher...")
        asyncio.create_task(publisher.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass  # Windows or non-main thread

    try:
        await publisher.run_loop()
    except Exception as exc:
        logger.critical(f"Outbox Publisher terminated with unhandled error: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
