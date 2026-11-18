from typing import Any

from job import create_job
from app.repositories.job_repository import JobRepository
from app.repositories.job_repository_interface import JobRepositoryInterface


class JobService:

    def __init__(self, job_repository: JobRepositoryInterface):
        self.job_repository = job_repository

    def create_job(self, task_type: str, payload: dict[str, Any]):
        job = create_job(task_type, payload)

        return self.job_repository.create(job)

    def get_job(self, job_id: str):
        return self.job_repository.get_by_id(job_id)

    def get_all_jobs(self):
        return self.job_repository.get_all()
