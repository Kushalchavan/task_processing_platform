from sqlalchemy.orm import Session

from job import Job, JobStatus
from app.models.job_model import JobModel


class PostgresJobRepository:

    def __init__(self, db: Session):
        self.db = db

    def create(self, job: Job) -> Job:
        job_model = JobModel(
            id=job.id,
            task_type=job.task_type,
            payload=job.payload,
            status=job.status.value,
            attempts=job.attempts,
        )

        self.db.add(job_model)
        self.db.flush()

        return job

    def get_by_id(self, job_id: str) -> Job | None:
        job_model = self.db.get(JobModel, job_id)

        if job_model is None:
            return None

        return Job(
            id=job_model.id,
            task_type=job_model.task_type,
            payload=job_model.payload,
            status=JobStatus(job_model.status),
            attempts=job_model.attempts,
        )

    def get_all(self) -> list[Job]:
        job_models = self.db.query(JobModel).all()

        return [
            Job(
                id=job.id,
                task_type=job.task_type,
                payload=job.payload,
                status=JobStatus(job.status),
                attempts=job.attempts,
            )
            for job in job_models
        ]

    def save(self, job: Job) -> Job:
        job_model = self.db.get(JobModel, job.id)

        if job_model is None:
            raise KeyError(f"Job not found: {job.id}")

        job_model.task_type = job.task_type
        job_model.payload = job.payload
        job_model.status = job.status.value
        job_model.attempts = job.attempts

        self.db.flush()

        return job

    def claim_next_job(self) -> Job | None:
        job_model = (
            self.db.query(JobModel)
            .filter(JobModel.status == JobStatus.PENDING.value)
            .order_by(JobModel.id)
            .with_for_update(skip_locked=True)
            .first()
        )

        if job_model is None:
            return None

        job_model.status = JobStatus.PROCESSING.value
        job_model.attempts += 1

        self.db.flush()

        return Job(
            id=job_model.id,
            task_type=job_model.task_type,
            payload=job_model.payload,
            status=JobStatus(job_model.status),
            attempts=job_model.attempts,
        )

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()
