from typing import Any

from job import create_job, JobStatus
from app.repositories.job_repository_interface import JobRepositoryInterface


class JobService:

    def __init__(self, job_repository: JobRepositoryInterface):
        self.job_repository = job_repository

    def create_job(self, task_type: str, payload: dict[str, Any]):
        job = create_job(task_type, payload)

        try:
            created_job = self.job_repository.create(job)
            self.job_repository.commit()
            return created_job

        except Exception:
            self.job_repository.rollback()
            raise

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

            if job is None:
                self.job_repository.commit()
                return None

            self.job_repository.commit()

            return job

        except Exception:
            self.job_repository.rollback()
            raise
