from datetime import datetime, timezone

from app.domain.job import Job, JobStatus


def test_job_can_be_scheduled_for_retry():
    job = Job(task_type="send_email", payload={})
    job.update_status(JobStatus.PROCESSING)
    job.attempts = 1

    retry_at = datetime.now(timezone.utc)

    result = job.schedule_retry("Connection timeout", retry_at)

    assert result is True
    assert job.status == JobStatus.PENDING
    assert job.next_retry_at == retry_at
    assert job.last_error == "Connection timeout"


def test_job_fails_when_max_attempts_reached():
    job = Job(task_type="send_email", payload={})
    job.update_status(JobStatus.PROCESSING)
    job.attempts = 3

    retry_at = datetime.now(timezone.utc)

    result = job.schedule_retry("Connection timeout", retry_at)

    assert result is False
    assert job.status == JobStatus.FAILED
    assert job.next_retry_at is None
    assert job.last_error == "Connection timeout"


def test_job_rejects_zero_max_attempts():
    try:
        Job(task_type="send_email", payload={}, max_attempts=0)
        assert False, "Expected ValueError"
    except ValueError:
        pass
