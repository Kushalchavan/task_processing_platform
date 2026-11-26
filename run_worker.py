import logging
import signal

from app.database.connection import SessionLocal
from app.repositories.postgres_job_repository import PostgresJobRepository
from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.workers.task_handlers import handle_print_message
from app.workers.worker import Worker
from app.queue.redis_stream import RedisStream

from prometheus_client import start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def main() -> None:
    db = SessionLocal()

    try:
        repository = PostgresJobRepository(db)
        redis_stream = RedisStream()

        # Ensure the consumer group exists before consuming messages.
        redis_stream.create_consumer_group()

        job_service = JobService(repository, redis_stream)

        registry = TaskHandlerRegistry()
        registry.register("print_message", handle_print_message)

        worker = Worker(
            job_service=job_service,
            task_registry=registry,
            redis_stream=redis_stream,
            consumer_name="worker-1",
        )

        # Expose Prometheus metrics on port 8001
        start_http_server(8001)
        logger.info("Worker metrics server started on port 8001")

        def shutdown(signum, frame):
            logger.info("Shutdown signal received")
            worker.shutdown_event.set()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        worker.run()

    finally:
        db.close()
        logger.info("Worker database session closed")


if __name__ == "__main__":
    main()
