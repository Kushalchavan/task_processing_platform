import pytest

from unittest.mock import Mock
from datetime import datetime, timedelta, timezone

from app.queue.redis_stream import RedisStream
from app.repositories.job_repository import JobRepository
from app.services.job_service import JobService
from app.domain.job import JobStatus, Job


def test_create_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = service.create_job(
        task_type="email",
        payload={"to": "test@example.com"},
    )

    assert job.task_type == "email"
    assert job.payload == {"to": "test@example.com"}
    assert job.status == JobStatus.PENDING
    assert repository.get_by_id(job.id) == job


def test_get_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    created_job = service.create_job("email", {})

    result = service.get_job(created_job.id)

    assert result == created_job


def test_get_nonexistent_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    result = service.get_job("invalid-id")

    assert result is None


def test_get_all_jobs():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    service.create_job("email", {})
    service.create_job("report", {})

    result = service.get_all_jobs()

    assert len(result) == 2


def test_claim_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)

    service = JobService(repository, redis_stream)

    job = service.create_job(
        "print_message",
        {"message": "Hello"},
    )

    claimed_job = service.claim_job(
        job_id=job.id,
        worker_id="worker-test",
    )

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.attempts == 1
    assert claimed_job.lease_owner == "worker-test"
    assert claimed_job.lease_expires_at is not None
    assert claimed_job.lease_expires_at > datetime.now(timezone.utc)
    assert claimed_job.lease_version == 1


def test_schedule_job_retry_first_attempt():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = service.create_job("email", {})

    claimed_job = service.claim_job(
        job_id=job.id,
        worker_id="worker-test",
    )
    assert claimed_job is not None

    before = datetime.now(timezone.utc)

    result = service.schedule_job_retry(
        job_id=job.id,
        worker_id="worker-test",
        lease_version=claimed_job.lease_version,
        error="Connection timeout",
    )

    after = datetime.now(timezone.utc)

    assert result is not None
    updated_job, should_retry = result

    assert should_retry is True
    assert updated_job.status == JobStatus.PENDING
    assert updated_job.attempts == 1
    assert updated_job.last_error == "Connection timeout"
    assert updated_job.next_retry_at is not None
    assert updated_job.lease_owner is None
    assert updated_job.lease_expires_at is None

    retry_at = updated_job.next_retry_at

    assert before + timedelta(seconds=5) <= retry_at
    assert retry_at <= after + timedelta(seconds=5)


def test_schedule_job_retry_second_attempt():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = service.create_job("email", {})

    first_claim = service.claim_job(
        job_id=job.id,
        worker_id="worker-test",
    )
    assert first_claim is not None

    first_result = service.schedule_job_retry(
        job_id=job.id,
        worker_id="worker-test",
        lease_version=first_claim.lease_version,
        error="First failure",
    )
    assert first_result is not None

    # Make the retry immediately eligible for the next test attempt.
    job.next_retry_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    second_claim = service.claim_job(
        job_id=job.id,
        worker_id="worker-test",
    )
    assert second_claim is not None

    before = datetime.now(timezone.utc)

    result = service.schedule_job_retry(
        job_id=job.id,
        worker_id="worker-test",
        lease_version=second_claim.lease_version,
        error="Second failure",
    )

    after = datetime.now(timezone.utc)

    assert result is not None
    updated_job, should_retry = result

    assert should_retry is True
    assert updated_job.attempts == 2
    assert updated_job.next_retry_at is not None
    assert updated_job.lease_owner is None
    assert updated_job.lease_expires_at is None

    retry_at = updated_job.next_retry_at

    assert before + timedelta(seconds=10) <= retry_at
    assert retry_at <= after + timedelta(seconds=10)


def test_schedule_job_retry_max_attempts_reached():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = Job(
        task_type="email",
        payload={},
        max_attempts=1,
    )
    repository.create(job)

    claimed_job = service.claim_job(
        job_id=job.id,
        worker_id="worker-test",
    )
    assert claimed_job is not None

    result = service.schedule_job_retry(
        job_id=job.id,
        worker_id="worker-test",
        lease_version=claimed_job.lease_version,
        error="Connection timeout",
    )

    assert result is not None
    updated_job, should_retry = result

    assert should_retry is False
    assert updated_job.status == JobStatus.FAILED
    assert updated_job.last_error == "Connection timeout"
    assert updated_job.next_retry_at is None
    assert updated_job.lease_owner is None
    assert updated_job.lease_expires_at is None


def test_schedule_retry_for_nonexistent_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    with pytest.raises(KeyError, match="Job not found"):
        service.schedule_job_retry(
            job_id="invalid-job-id",
            worker_id="worker-test",
            lease_version=1,
            error="Connection timeout",
        )


def test_recover_expired_jobs():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = Job(
        task_type="send_email",
        payload={"email": "test@example.com"},
    )

    repository.create(job)

    repository.recover_expired_jobs = Mock(return_value=[job])

    recovered_jobs = service.recover_expired_jobs(
        worker_id="worker-2",
        lease_duration_seconds=90,
        batch_size=5,
    )

    assert recovered_jobs == [job]

    repository.recover_expired_jobs.assert_called_once_with(
        worker_id="worker-2",
        lease_duration_seconds=90,
        batch_size=5,
    )


def test_recover_expired_jobs_returns_empty_list():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    repository.recover_expired_jobs = Mock(return_value=[])

    recovered_jobs = service.recover_expired_jobs(
        worker_id="worker-2",
    )

    assert recovered_jobs == []


def test_recover_expired_jobs_rolls_back_on_error():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    repository.recover_expired_jobs = Mock(side_effect=RuntimeError("Database error"))

    repository.rollback = Mock()

    with pytest.raises(RuntimeError, match="Database error"):
        service.recover_expired_jobs(worker_id="worker-2")

    repository.rollback.assert_called_once()


def test_complete_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = Job(task_type="send_email", payload={})
    repository.create(job)

    claimed_job = repository.claim_job(job.id, "worker-1")
    assert claimed_job is not None

    result = service.complete_job(
        job_id=job.id,
        worker_id="worker-1",
        lease_version=claimed_job.lease_version,
    )

    assert result is not None
    assert result.status == JobStatus.COMPLETED
    assert result.lease_owner is None
    assert result.lease_expires_at is None


def test_complete_job_rejects_wrong_worker():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = Job(task_type="send_email", payload={})
    repository.create(job)

    claimed_job = repository.claim_job(job.id, "worker-1")
    assert claimed_job is not None

    result = service.complete_job(
        job_id=job.id,
        worker_id="worker-2",
        lease_version=claimed_job.lease_version,
    )

    assert result is None
    assert job.status == JobStatus.PROCESSING
    assert job.lease_owner == "worker-1"
