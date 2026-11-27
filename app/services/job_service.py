from typing import Any

from datetime import datetime, timedelta, timezone

from app.domain.job import create_job, JobStatus, Job
from app.repositories.job_repository_interface import JobRepositoryInterface
from app.queue.redis_stream import RedisStream
from app.monitoring.metrics import JOBS_CREATED, JOBS_RETRIED


class JobService:

    def __init__(
        self,
        job_repository: JobRepositoryInterface,
        redis_stream: RedisStream,
    ):
        self.job_repository = job_repository
        self.redis_stream = redis_stream

    def create_job(self, task_type: str, payload: dict[str, Any]):
        job = create_job(task_type, payload)

        # First persist the job in PostgreSQL.
        try:
            created_job = self.job_repository.create(job)
            self.job_repository.commit()
        except Exception:
            self.job_repository.rollback()
            raise

        # Then publish its ID to Redis.
        self.redis_stream.publish_job(created_job.id)

        JOBS_CREATED.labels(task_type=created_job.task_type).inc()

        return created_job

    def get_job(self, job_id: str):
        return self.job_repository.get_by_id(job_id)

    def get_all_jobs(self):
        return self.job_repository.get_all()

    def update_job_status(self, job_id: str, status: JobStatus | str):
        try:
            job = self.job_repository.get_by_id(job_id)

            if job is None:
                raise KeyError(f"Job not found: {job_id}")

            job.update_status(status)
            updated_job = self.job_repository.save(job)
            self.job_repository.commit()

            return updated_job

        except Exception:
            self.job_repository.rollback()
            raise

    def claim_next_job(
        self,
        worker_id: str,
        lease_duration_seconds: int = 60,
    ) -> Job | None:
        try:
            job = self.job_repository.claim_next_job(
                worker_id=worker_id,
                lease_duration_seconds=lease_duration_seconds,
            )
            self.job_repository.commit()
            return job

        except Exception:
            self.job_repository.rollback()
            raise

    def claim_job(
        self,
        job_id: str,
        worker_id: str,
        lease_duration_seconds: int = 60,
    ) -> Job | None:
        try:
            job = self.job_repository.claim_job(
                job_id=job_id,
                worker_id=worker_id,
                lease_duration_seconds=lease_duration_seconds,
            )
            self.job_repository.commit()
            return job
        except Exception:
            self.job_repository.rollback()
            raise

    def schedule_job_retry(
        self,
        job_id: str,
        worker_id: str,
        lease_version: int,
        error: str,
        base_delay_seconds: int = 5,
    ) -> tuple[Job, bool] | None:
        """
        Schedule a retry only if the worker still owns the active lease.
        """

        if base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be positive")

        try:
            job = self.job_repository.get_by_id(job_id)

            if job is None:
                raise KeyError(f"Job not found: {job_id}")

            # Exponential backoff: 5s, 10s, 20s, 40s...
            delay_seconds = base_delay_seconds * (2 ** (job.attempts - 1))

            retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

            result = self.job_repository.schedule_retry(
                job_id=job_id,
                worker_id=worker_id,
                lease_version=lease_version,
                error=error,
                retry_at=retry_at,
            )

            self.job_repository.commit()

            if result is not None:
                updated_job, should_retry = result

                if should_retry:
                    JOBS_RETRIED.labels(task_type=updated_job.task_type).inc()

                return result

        except Exception:
            self.job_repository.rollback()
            raise

    def recover_expired_jobs(
        self,
        worker_id: str,
        lease_duration_seconds: int = 60,
        batch_size: int = 10,
    ) -> list[Job]:
        try:
            recovered_jobs = self.job_repository.recover_expired_jobs(
                worker_id=worker_id,
                lease_duration_seconds=lease_duration_seconds,
                batch_size=batch_size,
            )

            self.job_repository.commit()
            return recovered_jobs

        except Exception:
            self.job_repository.rollback()
            raise

    def complete_job(
        self,
        job_id: str,
        worker_id: str,
        lease_version: int,
    ) -> Job | None:
        """
        Complete a job only when the worker still owns its active lease.

        The repository performs the actual ownership and fencing checks.
        """

        try:
            completed_job = self.job_repository.complete_job(
                job_id=job_id,
                worker_id=worker_id,
                lease_version=lease_version,
            )

            self.job_repository.commit()
            return completed_job

        except Exception:
            self.job_repository.rollback()
            raise
