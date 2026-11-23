from sqlalchemy.orm import Session
from sqlalchemy import func, or_

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

        self.db.flush()

        return job

    def claim_next_job(self) -> Job | None:
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

        # The retry is now being executed, so it is no longer scheduled.
        job_model.next_retry_at = None

        self.db.flush()

        return self._to_domain(job_model)

    def claim_job(self, job_id: str) -> Job | None:
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

        # clear the scheduled retry timestamp once the job is claimed
        job_model.next_retry_at = None

        self.db.flush()

        return self._to_domain(job_model)

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()
