import logging

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
