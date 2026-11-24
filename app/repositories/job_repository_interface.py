from datetime import datetime
from typing import Protocol

from app.domain.job import Job


class JobRepositoryInterface(Protocol):
    def create(self, job: Job) -> Job: ...

    def get_by_id(self, job_id: str) -> Job | None: ...

    def get_all(self) -> list[Job]: ...

    def save(self, job: Job) -> Job: ...

    def claim_next_job(
        self, worker_id: str, lease_duration_seconds: int = 60
    ) -> Job | None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def claim_job(
        self, job_id: str, worker_id: str, lease_duration_seconds: int = 60
    ) -> Job | None: ...

    def recover_expired_jobs(
        self,
        worker_id: str,
        lease_duration_seconds: int = 60,
        batch_size: int = 10,
    ) -> list[Job]: ...

    def complete_job(
        self,
        job_id: str,
        worker_id: str,
        lease_version: int,
    ) -> Job | None: ...

    def schedule_retry(
        self,
        job_id: str,
        worker_id: str,
        lease_version: int,
        error: str,
        retry_at: datetime,
    ) -> tuple[Job, bool] | None: ...
