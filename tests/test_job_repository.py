from app.domain.job import Job
from app.repositories.job_repository import JobRepository


def test_create_job():
    repository = JobRepository()
    job = Job(task_type="email", payload={"to": "test@example.com"})

    result = repository.create(job)

    assert result == job
    assert repository.jobs[job.id] == job


def test_get_job_by_id():
    repository = JobRepository()
    job = Job(task_type="email", payload={})
    repository.create(job)

    result = repository.get_by_id(job.id)

    assert result == job


def test_get_nonexistent_job():
    repository = JobRepository()

    result = repository.get_by_id("invalid-id")

    assert result is None


def test_get_all_jobs():
    repository = JobRepository()
    job1 = Job(task_type="email", payload={})
    job2 = Job(task_type="report", payload={})

    repository.create(job1)
    repository.create(job2)

    result = repository.get_all()

    assert len(result) == 2
    assert job1 in result
    assert job2 in result
