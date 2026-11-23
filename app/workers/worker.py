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
        recovery_idle_time_ms: int = 60000,
    ) -> None:
        self.job_service = job_service
        self.task_registry = task_registry
        self.redis_stream = redis_stream
        self.consumer_name = consumer_name
        self.shutdown_event = threading.Event()
        self.recovery_idle_time_ms = recovery_idle_time_ms

    def process_one_job(self) -> bool:
        message = self.redis_stream.consume_one(self.consumer_name)

        if message is None:
            message = self.redis_stream.claim_one_pending(
                consumer_name=self.consumer_name,
                min_idle_time=self.recovery_idle_time_ms,
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

        except Exception as error:
            logger.exception("Job execution failed: %s", job.id)

            # Let JobService decide whether another attempt is allowed
            # and calculate the next retry time.
            updated_job, should_retry = self.job_service.schedule_job_retry(
                job_id=job.id,
                error=str(error),
            )

            if should_retry:
                # Keep retries out of the main stream until their delay expires.
                if updated_job.next_retry_at is None:
                    raise ValueError(f"Retry time missing for job {job.id}")

                self.redis_stream.schedule_delayed_job(
                    job_id=updated_job.id,
                    retry_at=updated_job.next_retry_at,
                )

                logger.info(
                    "Job %s scheduled for retry at %s",
                    job.id,
                    updated_job.next_retry_at,
                )
            else:
                logger.warning(
                    "Job %s exhausted its retry attempts",
                    job.id,
                )

        else:
            # Acknowledge only after the completion status is saved.
            self.job_service.update_job_status(
                job.id,
                JobStatus.COMPLETED,
            )

        # Acknowledge only after the outcome is persisted and,
        # when applicable, the delayed retry is scheduled.
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
