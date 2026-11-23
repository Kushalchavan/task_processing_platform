from unittest.mock import Mock
from app.queue.redis_stream import RedisStream

from app.repositories.job_repository import JobRepository
from app.services.job_service import JobService
from app.domain.job import JobStatus


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
