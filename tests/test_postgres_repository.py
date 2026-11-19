import pytest

from job import create_job, JobStatus
from app.repositories.postgres_job_repository import PostgresJobRepository


def test_create_job(db_session):
    repository = PostgresJobRepository(db_session)

    job = create_job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    created_job = repository.create(job)

    assert created_job.id == job.id
    assert created_job.task_type == "send_email"
    assert created_job.payload == {"email": "test@example.com"}
    assert created_job.status.value == "pending"
    assert created_job.attempts == 0


def test_get_job_by_id(db_session):
    repository = PostgresJobRepository(db_session)

    job = create_job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    repository.create(job)

    retrieved_job = repository.get_by_id(job.id)

    assert retrieved_job is not None
    assert retrieved_job.id == job.id
    assert retrieved_job.task_type == job.task_type
    assert retrieved_job.payload == job.payload
    assert retrieved_job.status == job.status


def test_get_all_jobs(db_session):
    repository = PostgresJobRepository(db_session)

    job1 = create_job(
        task_type="send_email",
        payload={"email": "first@example.com"},
    )

    job2 = create_job(
        task_type="generate_report",
        payload={"report_id": 123},
    )

    repository.create(job1)
    repository.create(job2)

    jobs = repository.get_all()

    assert len(jobs) == 2

    job_ids = {job.id for job in jobs}

    assert job1.id in job_ids
    assert job2.id in job_ids


def test_save_job_status(db_session):
    repository = PostgresJobRepository(db_session)

    job = create_job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    repository.create(job)

    job.update_status(JobStatus.PROCESSING)

    repository.save(job)

    retrieved_job = repository.get_by_id(job.id)

    assert retrieved_job is not None
    assert retrieved_job.status == JobStatus.PROCESSING


def test_get_nonexistent_job(db_session):
    repository = PostgresJobRepository(db_session)

    job = repository.get_by_id("nonexistent-job-id")

    assert job is None



def test_save_nonexistent_job(db_session):
    repository = PostgresJobRepository(db_session)

    job = create_job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    with pytest.raises(KeyError, match="Job not found"):
        repository.save(job)