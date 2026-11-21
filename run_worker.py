import logging
import signal

from app.database.connection import SessionLocal
from app.repositories.postgres_job_repository import PostgresJobRepository
from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.workers.task_handlers import handle_print_message
from app.workers.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def main() -> None:
    # Create a database session for this worker process.
    db = SessionLocal()

    try:
        repository = PostgresJobRepository(db)
        job_service = JobService(repository)

        registry = TaskHandlerRegistry()
        registry.register("print_message", handle_print_message)

        worker = Worker(job_service, registry)

        # Request graceful shutdown when the process receives a signal.
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
