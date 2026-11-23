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

    job = service.create_job(task_type="email", payload={"to": "test@example.com"})

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

    claimed_job = service.claim_job(job.id)

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.attempts == 1


def test_schedule_job_retry_first_attempt():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = service.create_job("email", {})
    service.claim_job(job.id)

    before = datetime.now(timezone.utc)

    updated_job, should_retry = service.schedule_job_retry(
        job.id,
        error="Connection timeout",
    )

    after = datetime.now(timezone.utc)

    # First failed attempt should schedule a retry after 5 seconds.
    assert should_retry is True
    assert updated_job.status == JobStatus.PENDING
    assert updated_job.attempts == 1
    assert updated_job.last_error == "Connection timeout"
    # A successful retry scheduling operation must set a retry timestamp.
    assert updated_job.next_retry_at is not None

    retry_at = updated_job.next_retry_at

    # Verify that the retry timestamp is approximately 5 seconds in the future.
    assert before + timedelta(seconds=5) <= retry_at
    assert retry_at <= after + timedelta(seconds=5)


def test_schedule_job_retry_second_attempt():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    job = service.create_job("email", {})

    # Simulate two failed execution attempts.
    service.claim_job(job.id)
    service.schedule_job_retry(job.id, "First failure")

    service.claim_job(job.id)

    before = datetime.now(timezone.utc)

    updated_job, should_retry = service.schedule_job_retry(
        job.id,
        error="Second failure",
    )

    after = datetime.now(timezone.utc)

    # Second failed attempt should use a 10-second delay.
    assert should_retry is True
    assert updated_job.attempts == 2
    # A successful retry scheduling operation must set a retry timestamp.
    assert updated_job.next_retry_at is not None

    retry_at = updated_job.next_retry_at

    # Verify that the retry timestamp is approximately 10 seconds in the future.
    assert before + timedelta(seconds=10) <= retry_at
    assert retry_at <= after + timedelta(seconds=10)


def test_schedule_job_retry_max_attempts_reached():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    # Configure a job that is allowed only one attempt.
    job = Job(
        task_type="email",
        payload={},
        max_attempts=1,
    )
    repository.create(job)

    service.claim_job(job.id)

    updated_job, should_retry = service.schedule_job_retry(
        job.id,
        error="Connection timeout",
    )

    assert should_retry is False
    assert updated_job.status == JobStatus.FAILED
    assert updated_job.last_error == "Connection timeout"
    assert updated_job.next_retry_at is None


def test_schedule_retry_for_nonexistent_job():
    repository = JobRepository()
    redis_stream = Mock(spec=RedisStream)
    service = JobService(repository, redis_stream)

    # A retry request for an unknown job should fail clearly.
    try:
        service.schedule_job_retry(
            "invalid-job-id",
            error="Connection timeout",
        )
        assert False, "Expected KeyError"
    except KeyError:
        pass
