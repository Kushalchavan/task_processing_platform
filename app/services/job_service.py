from typing import Any

from app.domain.job import create_job, JobStatus
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

    def claim_next_job(self):
        try:
            job = self.job_repository.claim_next_job()
            self.job_repository.commit()
            return job

        except Exception:
            self.job_repository.rollback()
            raise

    def claim_job(self, job_id: str):
        try:
            job = self.job_repository.claim_job(job_id)
            self.job_repository.commit()
            return job

        except Exception:
            self.job_repository.rollback()
            raise
