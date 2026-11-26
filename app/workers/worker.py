import logging
import threading
import time

from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.domain.job import JobStatus, Job
from app.queue.redis_stream import RedisStream
from app.monitoring.metrics import (
    JOBS_COMPLETED,
    JOBS_FAILED,
    JOB_PROCESSING_DURATION,
    WORKER_JOBS_IN_PROGRESS,
    WORKER_JOBS_PROCESSED,
)

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        job_service: JobService,
        task_registry: TaskHandlerRegistry,
        redis_stream: RedisStream,
        consumer_name: str,
        recovery_idle_time_ms: int = 60000,
        recovery_interval_seconds: int = 30,
        recovery_batch_size: int = 10,
    ) -> None:
        self.job_service = job_service
        self.task_registry = task_registry
        self.redis_stream = redis_stream
        self.consumer_name = consumer_name
        self.shutdown_event = threading.Event()
        self.recovery_idle_time_ms = recovery_idle_time_ms
        self.recovery_interval_seconds = recovery_interval_seconds
        self.recovery_batch_size = recovery_batch_size

        if recovery_idle_time_ms < 0:
            raise ValueError("recovery_idle_time_ms cannot be negative")

        if recovery_interval_seconds <= 0:
            raise ValueError("recovery_interval_seconds must be positive")

        if recovery_batch_size <= 0:
            raise ValueError("recovery_batch_size must be positive")

    def _handle_unclaimable_message(
        self,
        message_id: str,
        job_id: str,
    ) -> None:
        # Check PostgreSQL to understand why the claim failed.
        job = self.job_service.get_job(job_id)

        if job is None:
            logger.warning("Job does not exist: %s", job_id)
            self.redis_stream.acknowledge_job(message_id)
            return

        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            logger.info(
                "Acknowledging stale message for terminal job: %s",
                job_id,
            )
            self.redis_stream.acknowledge_job(message_id)
            return

        if job.status == JobStatus.PROCESSING:
            logger.info(
                "Job is already being processed: %s",
                job_id,
            )
            self.redis_stream.acknowledge_job(message_id)
            return

        if job.status == JobStatus.PENDING and job.next_retry_at is not None:
            # PostgreSQL is authoritative for the retry timestamp.
            self.redis_stream.schedule_delayed_job(
                job_id=job.id,
                retry_at=job.next_retry_at,
            )

            # Acknowledge only after Redis has accepted the reschedule.
            self.redis_stream.acknowledge_job(message_id)

            logger.info(
                "Premature message rescheduled for job %s at %s",
                job.id,
                job.next_retry_at,
            )
            return

        # For other unexpected states, leave the message pending.
        # Recovery can try again later.
        logger.warning(
            "Job %s could not be claimed; leaving message pending",
            job_id,
        )

    def _execute_claimed_job(
        self,
        job: Job,
        message_id: str | None = None,
    ) -> None:
        """
        Execute a job that is already claimed by this worker.
        """

        handler = self.task_registry.get_handler(job.task_type)

        WORKER_JOBS_IN_PROGRESS.inc()
        started_at = time.perf_counter()
        outcome = None

        try:
            try:
                handler(job.payload)

            except Exception as error:
                logger.exception("Job execution failed: %s", job.id)

                result = self.job_service.schedule_job_retry(
                    job_id=job.id,
                    worker_id=self.consumer_name,
                    lease_version=job.lease_version,
                    error=str(error),
                )

                if result is None:
                    outcome = "lease_lost"
                    logger.warning(
                        "Could not schedule retry because worker lost lease: %s",
                        job.id,
                    )
                    return

                updated_job, should_retry = result

                if should_retry:
                    outcome = "retry_scheduled"

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
                    outcome = "permanent_failure"
                    JOBS_FAILED.labels(task_type=job.task_type).inc()

                    logger.warning(
                        "Job %s exhausted its retry attempts",
                        job.id,
                    )

            else:
                completed_job = self.job_service.complete_job(
                    job_id=job.id,
                    worker_id=self.consumer_name,
                    lease_version=job.lease_version,
                )

                if completed_job is None:
                    outcome = "lease_lost"
                    logger.warning(
                        "Could not complete job because worker lost lease: %s",
                        job.id,
                    )
                    return

                outcome = "success"
                JOBS_COMPLETED.labels(task_type=job.task_type).inc()

                logger.info("Job completed successfully: %s", job.id)

        finally:
            duration = time.perf_counter() - started_at

            JOB_PROCESSING_DURATION.labels(task_type=job.task_type).observe(duration)

            WORKER_JOBS_IN_PROGRESS.dec()

            if outcome is not None:
                WORKER_JOBS_PROCESSED.labels(
                    task_type=job.task_type,
                    outcome=outcome,
                ).inc()

        # Recovered jobs don't have a newly consumed Redis message.
        if message_id is not None:
            self.redis_stream.acknowledge_job(message_id)

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

        job = self.job_service.claim_job(
            job_id=job_id,
            worker_id=self.consumer_name,
        )

        if job is None:
            self._handle_unclaimable_message(
                message_id=message_id,
                job_id=job_id,
            )
            return True

        self._execute_claimed_job(
            job=job,
            message_id=message_id,
        )

        return True

    def recover_expired_jobs(self) -> int:
        """Recover and execute jobs abandoned by workers."""

        recovered_jobs = self.job_service.recover_expired_jobs(
            worker_id=self.consumer_name,
            batch_size=self.recovery_batch_size,
        )

        if not recovered_jobs:
            return 0

        logger.info(
            "Recovered %d expired jobs for worker %s",
            len(recovered_jobs),
            self.consumer_name,
        )

        for job in recovered_jobs:
            self._execute_claimed_job(job=job)

        return len(recovered_jobs)

    def run(self) -> None:
        logger.info("Worker started: %s", self.consumer_name)

        next_recovery_at = 0.0

        while not self.shutdown_event.is_set():
            try:
                current_time = time.monotonic()

                if current_time >= next_recovery_at:
                    self.recover_expired_jobs()
                    next_recovery_at = time.monotonic() + self.recovery_interval_seconds

                self.process_one_job()

            except Exception:
                logger.exception("Unexpected worker error")

        logger.info("Worker stopped: %s", self.consumer_name)
