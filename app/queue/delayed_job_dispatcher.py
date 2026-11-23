import logging
import threading
from datetime import datetime, timezone

from app.queue.redis_stream import RedisStream

logger = logging.getLogger(__name__)


class DelayedJobDispatcher:
    def __init__(
        self,
        redis_stream: RedisStream,
        poll_interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        self.redis_stream = redis_stream
        self.poll_interval = poll_interval
        self.batch_size = batch_size

        # Allows the dispatcher to stop gracefully.
        self.shutdown_event = threading.Event()

    def dispatch_once(self) -> int:
        """Dispatch one batch of jobs whose retry time has arrived."""

        # Use timezone-aware UTC time for consistent scheduling.
        current_time = datetime.now(timezone.utc)

        dispatched = self.redis_stream.dispatch_due_jobs(
            current_time=current_time,
            count=self.batch_size,
        )

        if dispatched:
            logger.info("Dispatched %s delayed jobs", dispatched)

        return dispatched

    def run(self) -> None:
        """Continuously dispatch due jobs until shutdown is requested."""

        logger.info("Delayed job dispatcher started")

        while not self.shutdown_event.is_set():
            try:
                self.dispatch_once()
            except Exception:
                # Keep the dispatcher alive if Redis temporarily fails.
                logger.exception("Delayed job dispatch failed")

            # Wait between polling cycles, but allow immediate shutdown.
            self.shutdown_event.wait(self.poll_interval)

        logger.info("Delayed job dispatcher stopped")
