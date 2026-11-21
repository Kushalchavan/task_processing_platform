from app.repositories.job_repository import JobRepository
from app.services.job_service import JobService
from app.domain.job import JobStatus


def test_create_job():
    repository = JobRepository()
    service = JobService(repository)

    job = service.create_job(task_type="email", payload={"to": "test@example.com"})

    assert job.task_type == "email"
    assert job.payload == {"to": "test@example.com"}
    assert job.status == JobStatus.PENDING
    assert repository.get_by_id(job.id) == job


def test_get_job():
    repository = JobRepository()
    service = JobService(repository)

    created_job = service.create_job("email", {})

    result = service.get_job(created_job.id)

    assert result == created_job


def test_get_nonexistent_job():
    repository = JobRepository()
    service = JobService(repository)

    result = service.get_job("invalid-id")

    assert result is None


def test_get_all_jobs():
    repository = JobRepository()
    service = JobService(repository)

    service.create_job("email", {})
    service.create_job("report", {})

    result = service.get_all_jobs()

    assert len(result) == 2
