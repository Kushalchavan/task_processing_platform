import pytest

from job_manager import JobManager


def test_create_and_store_job():
    manager = JobManager()

    job = manager.create_job("send_email", {"email", "john@gmail.com"})

    assert job["status"] == "pending"
    assert manager.get_job(job["id"]) == job


def test_get_all_jobs():
    manager = JobManager()

    manager.create_job("send_email", {})
    manager.create_job("generate_report", {})

    jobs = manager.get_all_jobs()

    assert len(jobs) == 2


def test_update_job_status():
    manager = JobManager()

    job = manager.create_job("send_email", {})
    updated_job = manager.update_job_status(job["id"], "processing")

    assert updated_job["status"] == "processing"


def test_update_nonexistent_job():
    manager = JobManager()

    with pytest.raises(KeyError):
        manager.update_job_status("unknown-id", "processing")


def test_invalid_status():
    manager = JobManager()

    job = manager.create_job("send_email", {})

    with pytest.raises(ValueError):
        manager.update_job_status(job["id"], "banana")
