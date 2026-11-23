import logging
import threading

from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.domain.job import JobStatus
from app.queue.redis_stream import RedisStream

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        job_service: JobService,
        task_registry: TaskHandlerRegistry,
        redis_stream: RedisStream,
        consumer_name: str,
    ) -> None:
        self.job_service = job_service
        self.task_registry = task_registry
        self.redis_stream = redis_stream
        self.consumer_name = consumer_name
        self.shutdown_event = threading.Event()

    def process_one_job(self) -> bool:
        message = self.redis_stream.consume_one(self.consumer_name)

        if message is None:
            message = self.redis_stream.claim_one_pending(
                consumer_name=self.consumer_name,
                min_idle_time=60000,
            )

        if message is None:
            return False

        message_id = message["message_id"]
        job_id = message["job_id"]

        # Claim the exact job referenced by the Redis message.
        job = self.job_service.claim_job(job_id)

        if job is None:
            logger.warning("Could not claim job: %s", job_id)
            # Leave the message pending for investigation/recovery.
            return True

        try:
            handler = self.task_registry.get_handler(job.task_type)
            handler(job.payload)

        except Exception:
            logger.exception("Job execution failed: %s", job.id)

            # Acknowledge only after the failure status is saved.
            self.job_service.update_job_status(
                job.id,
                JobStatus.FAILED,
            )

        else:
            # Acknowledge only after the completion status is saved.
            self.job_service.update_job_status(
                job.id,
                JobStatus.COMPLETED,
            )

        self.redis_stream.acknowledge_job(message_id)
        return True

    def run(self) -> None:
        logger.info("Worker started: %s", self.consumer_name)

        while not self.shutdown_event.is_set():
            try:
                self.process_one_job()
            except Exception:
                logger.exception("Unexpected worker error")

        logger.info("Worker stopped: %s", self.consumer_name)
