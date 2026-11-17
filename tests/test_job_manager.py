import pytest

from job_manager import JobManager
from job import JobStatus


def test_pending_to_processing():
    manager = JobManager()
    job = manager.create_job("send_email", {})

    updated = manager.update_job_status(job.id, JobStatus.PROCESSING)

    assert updated.status == JobStatus.PROCESSING


def test_processing_to_completed():
    manager = JobManager()
    job = manager.create_job("send_email", {})

    manager.update_job_status(job.id, JobStatus.PROCESSING)

    updated = manager.update_job_status(job.id, JobStatus.COMPLETED)

    assert updated.status == JobStatus.COMPLETED


def test_processing_to_failed():
    manager = JobManager()
    job = manager.create_job("send_email", {})

    manager.update_job_status(job.id, JobStatus.PROCESSING)

    updated = manager.update_job_status(job.id, JobStatus.FAILED)

    assert updated.status == JobStatus.FAILED


def test_pending_cannot_move_directly_to_completed():
    manager = JobManager()
    job = manager.create_job("send_email", {})

    with pytest.raises(ValueError):
        manager.update_job_status(job.id, JobStatus.COMPLETED)


def test_completed_job_cannot_restart():
    manager = JobManager()
    job = manager.create_job("send_email", {})

    manager.update_job_status(job.id, JobStatus.PROCESSING)
    manager.update_job_status(job.id, JobStatus.COMPLETED)

    with pytest.raises(ValueError):
        manager.update_job_status(job.id, JobStatus.PROCESSING)


def test_invalid_status_is_rejected():
    manager = JobManager()
    job = manager.create_job("send_email", {})

    with pytest.raises(ValueError):
        manager.update_job_status(job.id, "banana")
