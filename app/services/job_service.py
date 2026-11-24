from typing import Any

from datetime import datetime, timedelta, timezone

from app.domain.job import create_job, JobStatus, Job
from app.repositories.job_repository_interface import JobRepositoryInterface
from app.queue.redis_stream import RedisStream


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
        error: str,
        base_delay_seconds: int = 5,
    ):
        try:
            job = self.job_repository.get_by_id(job_id)

            if job is None:
                raise KeyError(f"Job not found: {job_id}")

            # Exponential backoff: increase the delay after each failed attampt.
            # Ex: 5s, 10s, 20s, 40s
            delay_seconds = base_delay_seconds * (2 ** (job.attempts - 1))

            # Store the earliest time at which this job can be retried.
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

            # The domain decides wheather another attempss is allowed.
            should_retry = job.schedule_retry(
                error=error,
                retry_at=retry_at,
            )

            # Persist the updated status, retry timestamp and erro details
            updated_job = self.job_repository.save(job)
            self.job_repository.commit()

            return updated_job, should_retry

        except Exception:
            # Roll back the transaction if retrieving or updating the job fails.
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
