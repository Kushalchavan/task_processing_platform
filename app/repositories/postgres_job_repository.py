from sqlalchemy.orm import Session
from sqlalchemy import func, or_, update, case

from datetime import datetime

from app.domain.job import Job, JobStatus
from app.models.job_model import JobModel


class PostgresJobRepository:

    def __init__(self, db: Session):
        self.db = db

    def _to_domain(self, job_model: JobModel) -> Job:
        """Convert a SQLAlchemy model into a domain Job."""
        return Job(
            id=job_model.id,
            task_type=job_model.task_type,
            payload=job_model.payload,
            status=JobStatus(job_model.status),
            attempts=job_model.attempts,
            max_attempts=job_model.max_attempts,
            next_retry_at=job_model.next_retry_at,
            last_error=job_model.last_error,
            lease_owner=job_model.lease_owner,
            lease_expires_at=job_model.lease_expires_at,
            lease_version=job_model.lease_version,
        )

    def create(self, job: Job) -> Job:
        job_model = JobModel(
            id=job.id,
            task_type=job.task_type,
            payload=job.payload,
            status=job.status.value,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            next_retry_at=job.next_retry_at,
            last_error=job.last_error,
            lease_owner=job.lease_owner,
            lease_expires_at=job.lease_expires_at,
            lease_version=job.lease_version,
        )

        self.db.add(job_model)
        self.db.flush()

        return job

    def get_by_id(self, job_id: str) -> Job | None:
        job_model = self.db.get(JobModel, job_id)

        if job_model is None:
            return None

        return self._to_domain(job_model)

    def get_all(self) -> list[Job]:
        job_models = self.db.query(JobModel).all()

        return [self._to_domain(job) for job in job_models]

    def save(self, job: Job) -> Job:
        job_model = self.db.get(JobModel, job.id)

        if job_model is None:
            raise KeyError(f"Job not found: {job.id}")

        job_model.task_type = job.task_type
        job_model.payload = job.payload
        job_model.status = job.status.value
        job_model.attempts = job.attempts
        job_model.max_attempts = job.max_attempts
        job_model.next_retry_at = job.next_retry_at
        job_model.last_error = job.last_error
        job_model.lease_owner = job.lease_owner
        job_model.lease_expires_at = job.lease_expires_at
        job_model.lease_version = job.lease_version

        self.db.flush()

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

        job_model = (
            self.db.query(JobModel)
            .filter(
                JobModel.status == JobStatus.PENDING.value,
                or_(
                    JobModel.next_retry_at.is_(None),
                    JobModel.next_retry_at <= func.now(),
                ),
            )
            .order_by(JobModel.id)
            .with_for_update(skip_locked=True)
            .first()
        )

        if job_model is None:
            return None

        job_model.status = JobStatus.PROCESSING.value
        job_model.attempts += 1
        job_model.next_retry_at = None

        job_model.lease_owner = worker_id
        job_model.lease_expires_at = func.now() + func.make_interval(
            0, 0, 0, 0, 0, 0, lease_duration_seconds
        )
        job_model.lease_version += 1

        self.db.flush()

        return self._to_domain(job_model)

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

        job_model = (
            self.db.query(JobModel)
            .filter(
                JobModel.id == job_id,
                JobModel.status == JobStatus.PENDING.value,
                or_(
                    JobModel.next_retry_at.is_(None),
                    JobModel.next_retry_at <= func.now(),
                ),
            )
            .with_for_update(skip_locked=True)
            .first()
        )

        if job_model is None:
            return None

        job_model.status = JobStatus.PROCESSING.value
        job_model.attempts += 1
        job_model.next_retry_at = None

        # Assign temporary ownership to this worker.
        job_model.lease_owner = worker_id
        job_model.lease_expires_at = func.now() + func.make_interval(
            0, 0, 0, 0, 0, 0, lease_duration_seconds
        )

        # Increment the fencing token for this ownership generation.
        job_model.lease_version += 1

        self.db.flush()

        return self._to_domain(job_model)

    def recover_expired_jobs(
        self,
        worker_id: str,
        lease_duration_seconds: int = 60,
        batch_size: int = 10,
    ) -> list[Job]:
        """Reclaim jobs whose processing leases have expired."""

        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")

        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        expired_jobs = (
            self.db.query(JobModel)
            .filter(
                JobModel.status == JobStatus.PROCESSING.value,
                or_(
                    JobModel.lease_expires_at.is_(None),
                    JobModel.lease_expires_at <= func.now(),
                ),
            )
            .order_by(JobModel.lease_expires_at, JobModel.id)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
            .all()
        )

        recovered_job_models = []

        for job_model in expired_jobs:

            if job_model.attempts >= job_model.max_attempts:
                job_model.status = JobStatus.FAILED.value
                job_model.last_error = "Maximum attempts reached after lease expiration"
                job_model.lease_owner = None
                job_model.lease_expires_at = None
                continue

            job_model.attempts += 1
            job_model.lease_owner = worker_id

            job_model.lease_expires_at = func.now() + func.make_interval(
                0, 0, 0, 0, 0, 0, lease_duration_seconds
            )

            job_model.lease_version += 1

            recovered_job_models.append(job_model)

        # Execute the SQL expressions before converting models to domain objects.
        self.db.flush()

        return [self._to_domain(job_model) for job_model in recovered_job_models]

    def complete_job(
        self,
        job_id: str,
        worker_id: str,
        lease_version: int,
    ) -> Job | None:
        """
        Complete a job only if the worker still owns its active lease.

        The ownership and fencing-token checks happen atomically
        with the database update.
        """

        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")

        if lease_version < 1:
            raise ValueError("lease_version must be positive")

        statement = (
            update(JobModel)
            .where(
                JobModel.id == job_id,
                JobModel.status == JobStatus.PROCESSING.value,
                JobModel.lease_owner == worker_id,
                JobModel.lease_version == lease_version,
                JobModel.lease_expires_at > func.now(),
            )
            .values(
                status=JobStatus.COMPLETED.value,
                lease_owner=None,
                lease_expires_at=None,
            )
            .returning(JobModel.id)
        )

        completed_job_id = self.db.execute(statement).scalar_one_or_none()

        if completed_job_id is None:
            return None

        self.db.expire_all()
        return self.get_by_id(completed_job_id)

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()

    def schedule_retry(
        self,
        job_id: str,
        worker_id: str,
        lease_version: int,
        error: str,
        retry_at: datetime,
    ) -> tuple[Job, bool] | None:
        """
        Schedule a retry only if the worker still owns the active lease.

        The ownership checks and retry state transition happen atomically.
        """

        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")

        if lease_version < 1:
            raise ValueError("lease_version must be positive")

        if retry_at.tzinfo is None or retry_at.utcoffset() is None:
            raise ValueError("retry_at must be timezone-aware")

        # Determine the next state using the current attempt count.
        should_retry = JobModel.attempts < JobModel.max_attempts

        statement = (
            update(JobModel)
            .where(
                JobModel.id == job_id,
                JobModel.status == JobStatus.PROCESSING.value,
                JobModel.lease_owner == worker_id,
                JobModel.lease_version == lease_version,
                JobModel.lease_expires_at > func.now(),
            )
            .values(
                status=case(
                    (should_retry, JobStatus.PENDING.value),
                    else_=JobStatus.FAILED.value,
                ),
                next_retry_at=case(
                    (should_retry, retry_at),
                    else_=None,
                ),
                last_error=error,
                lease_owner=None,
                lease_expires_at=None,
            )
            .returning(JobModel.id, JobModel.status)
        )

        result = self.db.execute(statement).one_or_none()

        # No matching row means the worker no longer owns the job.
        if result is None:
            return None

        updated_job_id, updated_status = result

        self.db.expire_all()

        updated_job = self.get_by_id(updated_job_id)

        if updated_job is None:
            return None

        return updated_job, updated_status == JobStatus.PENDING.value