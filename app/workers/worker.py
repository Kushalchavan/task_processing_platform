import logging
import time
import threading

from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from job import JobStatus

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        job_service: JobService,
        task_registry: TaskHandlerRegistry,
    ) -> None:
        self.job_service = job_service
        self.task_registry = task_registry
        self.shutdown_event = threading.Event()

    def process_one_job(self) -> bool:
        job = self.job_service.claim_next_job()

        if job is None:
            return False

        try:
            handler = self.task_registry.get_handler(job.task_type)

            handler(job.payload)

            self.job_service.update_job_status(
                job.id,
                JobStatus.COMPLETED,
            )

            return True

        except Exception:
            logger.exception("Job execution failed: %s", job.id)

            self.job_service.update_job_status(
                job.id,
                JobStatus.FAILED,
            )
            return True

    def run(self, poll_interval: float = 2.0) -> None:
        logger.info("Worker started")

        while not self.shutdown_event.is_set():
            job_processed = self.process_one_job()

            if not job_processed:
                # Wait for new work oa a shutdown signal
                self.shutdown_event.wait(timeout=poll_interval)

        logger.info("Worker stopped")
