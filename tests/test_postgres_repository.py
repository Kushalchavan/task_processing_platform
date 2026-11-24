import pytest

from datetime import datetime, timezone, timedelta

from app.domain.job import create_job, JobStatus, Job
from app.repositories.postgres_job_repository import PostgresJobRepository
from app.models.job_model import JobModel


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
    claimed_job = repository.claim_next_job(worker_id="worker-2")

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

    claimed_job = repository.claim_job(job_id=job.id, worker_id="worker-1")

    assert claimed_job is not None
    assert claimed_job.id == job.id
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.attempts == 1

    # Verify lease ownership.
    assert claimed_job.lease_owner == "worker-1"
    assert claimed_job.lease_expires_at is not None
    assert claimed_job.lease_expires_at > datetime.now(timezone.utc)
    assert claimed_job.lease_version == 1


def test_claim_unknown_job_returns_none(db_session):
    repository = PostgresJobRepository(db_session)

    claimed_job = repository.claim_job(job_id="non-existent-job", worker_id="worker-1")

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

    claimed_job = repository.claim_job(job_id=job.id, worker_id="worker-1")

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

    claimed_job = repository.claim_job(job_id=job.id, worker_id="worker-1")

    assert claimed_job is None


def test_claim_job_twice_returns_none_on_second_attempt(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="print_message",
        payload={"message": "Hello"},
    )

    repository.create(job)
    repository.commit()

    first_claim = repository.claim_job(job_id=job.id, worker_id="worker-1")
    repository.commit()

    second_claim = repository.claim_job(job_id=job.id, worker_id="worker-2")

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


def test_claim_job_returns_none_when_retry_is_not_due(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    repository.create(job)

    job.update_status(JobStatus.PROCESSING)
    job.attempts = 1
    job.schedule_retry(
        error="Temporary failure",
        retry_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    repository.save(job)
    repository.commit()

    claimed_job = repository.claim_job(job_id=job.id, worker_id="worker-1")

    assert claimed_job is None

    unchanged_job = repository.get_by_id(job.id)

    assert unchanged_job is not None
    assert unchanged_job.status == JobStatus.PENDING
    assert unchanged_job.attempts == 1
    assert unchanged_job.next_retry_at is not None


def test_claim_job_succeeds_when_retry_is_due(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    repository.create(job)

    job.update_status(JobStatus.PROCESSING)
    job.attempts = 1
    job.schedule_retry(
        error="Temporary failure",
        retry_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )

    repository.save(job)
    repository.commit()

    claimed_job = repository.claim_job(job_id=job.id, worker_id="worker-1")

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.attempts == 2
    assert claimed_job.next_retry_at is None


def test_claim_next_job_skips_jobs_with_future_retry(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    repository.create(job)

    job.update_status(JobStatus.PROCESSING)
    job.attempts = 1
    job.schedule_retry(
        error="Temporary failure",
        retry_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    repository.save(job)
    repository.commit()

    claimed_job = repository.claim_next_job(worker_id="worker-2")

    assert claimed_job is None


def test_recover_expired_job(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
        max_attempts=3,
    )

    repository.create(job)

    # Simulate a worker that claimed the job but whose lease expired.
    job_model = db_session.get(
        __import__("app.models.job_model", fromlist=["JobModel"]).JobModel,
        job.id,
    )
    job_model.status = JobStatus.PROCESSING.value
    job_model.attempts = 1
    job_model.lease_owner = "worker-1"
    job_model.lease_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    job_model.lease_version = 1
    repository.commit()

    recovered_jobs = repository.recover_expired_jobs(
        worker_id="worker-2",
    )

    assert len(recovered_jobs) == 1

    recovered_job = recovered_jobs[0]
    assert recovered_job.id == job.id
    assert recovered_job.status == JobStatus.PROCESSING
    assert recovered_job.attempts == 2
    assert recovered_job.lease_owner == "worker-2"
    assert recovered_job.lease_expires_at is not None
    assert recovered_job.lease_expires_at > datetime.now(timezone.utc)
    assert recovered_job.lease_version == 2


def test_recover_expired_jobs_returns_empty_when_none_expired(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(task_type="send_email", payload={"email": "test@example.com"})
    repository.create(job)

    job_model = db_session.get(JobModel, job.id)
    job_model.status = JobStatus.PROCESSING.value
    job_model.attempts = 1
    job_model.lease_owner = "worker-1"
    job_model.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    repository.commit()

    recovered_jobs = repository.recover_expired_jobs(
        worker_id="worker-2",
    )

    assert recovered_jobs == []


def test_recover_expired_job_marks_failed_when_attempts_exhausted(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
        max_attempts=2,
    )
    repository.create(job)

    job_model = db_session.get(JobModel, job.id)
    job_model.status = JobStatus.PROCESSING.value
    job_model.attempts = 2
    job_model.lease_owner = "worker-1"
    job_model.lease_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    job_model.lease_version = 1
    repository.commit()

    recovered_jobs = repository.recover_expired_jobs(
        worker_id="worker-2",
    )

    assert recovered_jobs == []

    failed_job = repository.get_by_id(job.id)
    assert failed_job is not None
    assert failed_job.status == JobStatus.FAILED
    assert failed_job.attempts == 2
    assert failed_job.lease_owner is None
    assert failed_job.lease_expires_at is None
    assert failed_job.last_error is not None


def test_recover_expired_jobs_respects_batch_size(db_session):
    repository = PostgresJobRepository(db_session)

    jobs = [
        Job(task_type="send_email", payload={"email": f"{i}@example.com"})
        for i in range(3)
    ]

    for job in jobs:
        repository.create(job)
        job_model = db_session.get(JobModel, job.id)
        job_model.status = JobStatus.PROCESSING.value
        job_model.attempts = 1
        job_model.lease_owner = "worker-1"
        job_model.lease_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)

    repository.commit()

    recovered_jobs = repository.recover_expired_jobs(
        worker_id="worker-2",
        batch_size=2,
    )

    assert len(recovered_jobs) == 2


def test_recover_expired_jobs_increments_fencing_token(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(task_type="send_email", payload={"email": "test@example.com"})
    repository.create(job)

    job_model = db_session.get(JobModel, job.id)
    job_model.status = JobStatus.PROCESSING.value
    job_model.attempts = 1
    job_model.lease_owner = "worker-1"
    job_model.lease_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    job_model.lease_version = 7
    repository.commit()

    recovered_jobs = repository.recover_expired_jobs(
        worker_id="worker-2",
    )

    assert recovered_jobs[0].lease_version == 8


def test_recover_expired_jobs_rejects_invalid_arguments(db_session):
    repository = PostgresJobRepository(db_session)

    with pytest.raises(ValueError, match="worker_id"):
        repository.recover_expired_jobs(worker_id=" ")

    with pytest.raises(ValueError, match="lease_duration_seconds"):
        repository.recover_expired_jobs(
            worker_id="worker-1",
            lease_duration_seconds=0,
        )

    with pytest.raises(ValueError, match="batch_size"):
        repository.recover_expired_jobs(
            worker_id="worker-1",
            batch_size=0,
        )


def test_complete_job_with_valid_lease(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(task_type="send_email", payload={"email": "test@example.com"})
    repository.create(job)
    repository.commit()

    claimed_job = repository.claim_job(job.id, "worker-1")
    assert claimed_job is not None
    repository.commit()

    completed_job = repository.complete_job(
        job_id=job.id,
        worker_id="worker-1",
        lease_version=claimed_job.lease_version,
    )

    assert completed_job is not None
    assert completed_job.status == JobStatus.COMPLETED
    assert completed_job.lease_owner is None
    assert completed_job.lease_expires_at is None


def test_complete_job_rejects_wrong_worker(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(task_type="send_email", payload={})
    repository.create(job)
    repository.commit()

    repository.claim_job(job.id, "worker-1")
    repository.commit()

    result = repository.complete_job(job.id, "worker-2", 1)

    assert result is None


def test_complete_job_rejects_expired_lease(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(task_type="send_email", payload={})
    repository.create(job)
    repository.commit()

    repository.claim_job(job.id, "worker-1")
    repository.commit()

    job_model = db_session.get(JobModel, job.id)
    job_model.lease_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    repository.commit()

    result = repository.complete_job(job.id, "worker-1", 1)

    assert result is None


def test_complete_job_rejects_old_fencing_token(db_session):
    repository = PostgresJobRepository(db_session)

    job = Job(task_type="send_email", payload={})
    repository.create(job)
    repository.commit()

    # First worker acquires lease with fencing token 1.
    first_claim = repository.claim_job(job.id, "worker-1")
    repository.commit()

    assert first_claim is not None

    repository.commit()

    assert first_claim.lease_version == 1

    # Simulate lease expiration and recovery by another worker.
    db_session.query(JobModel).filter(JobModel.id == job.id).update(
        {JobModel.lease_expires_at: datetime.now(timezone.utc) - timedelta(seconds=10)}
    )
    repository.commit()

    second_claim = repository.recover_expired_jobs(worker_id="worker-2")
    repository.commit()

    assert len(second_claim) == 1
    assert second_claim[0].lease_version == 2

    # The old worker must not complete using its previous token.
    result = repository.complete_job(
        job.id,
        "worker-1",
        first_claim.lease_version,
    )

    assert result is None

    current_job = repository.get_by_id(job.id)
    assert current_job is not None

    assert current_job.status == JobStatus.PROCESSING
    assert current_job.lease_owner == "worker-2"
    assert current_job.lease_version == 2
