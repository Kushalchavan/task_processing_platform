import pytest

from datetime import datetime, timezone

from app.domain.job import create_job, JobStatus, Job
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


def test_claim_next_job_returns_none_when_no_pending_jobs(db_session):
    repository = PostgresJobRepository(db_session)

    # 1. Create a job
    job = Job(task_type="email", payload={"to": "test@example.com"})

    repository.create(job)

    # 2. Change its status to processing
    job.update_status(JobStatus.PROCESSING)
    repository.save(job)

    # 3. Call claim_next_job()
    claimed_job = repository.claim_next_job()

    # 4. Assert the result is None
    assert claimed_job is None


def test_create_job_rollback(db_session):
    repository = PostgresJobRepository(db_session)

    job = create_job(
        task_type="send_email",
        payload={"email": "rollback@example.com"},
    )

    # Insert the job without committing.
    repository.create(job)

    # Roll back the transaction.
    repository.rollback()

    # The job should no longer exist.
    retrieved_job = repository.get_by_id(job.id)

    assert retrieved_job is None


def test_claim_job_returns_processing_job(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus"},
    )

    repository.create(job)
    repository.commit()

    claimed_job = repository.claim_job(job.id)

    assert claimed_job is not None
    assert claimed_job.id == job.id
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.attempts == 1


def test_claim_unknown_job_returns_none(db_session):
    repository = PostgresJobRepository(db_session)

    claimed_job = repository.claim_job("non-existent-job")

    assert claimed_job is None


def test_claim_processing_job_returns_none(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="print_message",
        payload={"message": "Hello"},
    )

    repository.create(job)
    repository.commit()

    job.update_status(JobStatus.PROCESSING)
    repository.save(job)
    repository.commit()

    claimed_job = repository.claim_job(job.id)

    assert claimed_job is None


def test_claim_completed_job_returns_none(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="print_message",
        payload={"message": "Hello"},
    )

    repository.create(job)
    repository.commit()

    job.update_status(JobStatus.PROCESSING)
    job.update_status(JobStatus.COMPLETED)

    repository.save(job)
    repository.commit()

    claimed_job = repository.claim_job(job.id)

    assert claimed_job is None


def test_claim_job_twice_returns_none_on_second_attempt(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="print_message",
        payload={"message": "Hello"},
    )

    repository.create(job)
    repository.commit()

    first_claim = repository.claim_job(job.id)
    repository.commit()

    second_claim = repository.claim_job(job.id)

    assert first_claim is not None
    assert first_claim.status == JobStatus.PROCESSING

    assert second_claim is None


def test_retry_fields_are_persisted_and_retrieved(db_session):
    repository = PostgresJobRepository(db_session)

    retry_at = datetime.now(timezone.utc)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
        max_attempts=5,
    )

    repository.create(job)

    job.update_status(JobStatus.PROCESSING)
    job.attempts = 1

    job.schedule_retry(
        error="Connection timeout",
        retry_at=retry_at,
    )

    repository.save(job)
    repository.commit()

    retrieved_job = repository.get_by_id(job.id)

    assert retrieved_job is not None
    assert retrieved_job.max_attempts == 5
    assert retrieved_job.attempts == 1
    assert retrieved_job.status == JobStatus.PENDING
    assert retrieved_job.next_retry_at == retry_at
    assert retrieved_job.last_error == "Connection timeout"
