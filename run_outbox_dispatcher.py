import logging
import signal

from app.queue.redis_stream import RedisStream
from app.queue.outbox_dispatcher import OutboxDispatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def main() -> None:
    redis_stream = RedisStream()

    dispatcher = OutboxDispatcher(
        redis_stream=redis_stream,
        poll_interval=1.0,
        batch_size=10,
        claim_duration_seconds=60,
    )

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received")
        dispatcher.stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    dispatcher.run()


if __name__ == "__main__":
    main()
