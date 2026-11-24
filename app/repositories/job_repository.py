from datetime import datetime, timedelta, timezone

from app.domain.job import Job, JobStatus


class JobRepository:

    def __init__(self):
        self.jobs: dict[str, Job] = {}

    def create(self, job: Job) -> Job:
        self.jobs[job.id] = job
        return job

    def get_by_id(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def get_all(self) -> list[Job]:
        return list(self.jobs.values())

    def save(self, job: Job) -> Job:
        self.jobs[job.id] = job
        return job

    def claim_next_job(
        self,
        worker_id: str,
        lease_duration_seconds: int = 60,
    ) -> Job | None:

        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")

        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")

        for job in self.jobs.values():
            if job.status != JobStatus.PENDING:
                continue

            if job.next_retry_at is not None and job.next_retry_at > datetime.now(
                timezone.utc
            ):
                continue

            job.update_status(JobStatus.PROCESSING)
            job.attempts += 1
            job.next_retry_at = None

            job.lease_owner = worker_id
            job.lease_expires_at = datetime.now(timezone.utc) + timedelta(
                seconds=lease_duration_seconds
            )
            job.lease_version += 1

            return job

        return None

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def claim_job(
        self,
        job_id: str,
        worker_id: str,
        lease_duration_seconds: int = 60,
    ) -> Job | None:

        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")

        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")

        job = self.jobs.get(job_id)

        if job is None:
            return None

        if job.status != JobStatus.PENDING:
            return None

        # Assign ownership and expiration when the job is claimed.
        job.update_status(JobStatus.PROCESSING)
        job.attempts += 1
        job.lease_owner = worker_id
        job.lease_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=lease_duration_seconds
        )

        # Each new ownership generation gets a higher fencing token.
        job.lease_version += 1

        job.next_retry_at = None

        return job

    def recover_expired_jobs(
        self,
        worker_id: str,
        lease_duration_seconds: int = 60,
        batch_size: int = 10,
    ) -> list[Job]:

        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")

        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        now = datetime.now(timezone.utc)
        recovered_jobs = []

        expired_jobs = [
            job
            for job in self.jobs.values()
            if (
                job.status == JobStatus.PROCESSING
                and (job.lease_expires_at is None or job.lease_expires_at <= now)
            )
        ]

        # Process only the requested number of expired jobs.
        expired_jobs.sort(
            key=lambda job: (
                job.lease_expires_at is not None,
                job.lease_expires_at or now,
                job.id,
            )
        )

        for job in expired_jobs[:batch_size]:

            if job.attempts >= job.max_attempts:
                job.update_status(JobStatus.FAILED)
                job.last_error = "Maximum attempts reached after lease expiration"
                job.lease_owner = None
                job.lease_expires_at = None
                continue

            job.attempts += 1
            job.lease_owner = worker_id
            job.lease_expires_at = now + timedelta(seconds=lease_duration_seconds)
            job.lease_version += 1

            recovered_jobs.append(job)

        return recovered_jobs
