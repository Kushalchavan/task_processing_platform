from app.domain.job import Job, JobStatus


def test_pending_to_processing():
    job = Job(task_type="email", payload={})

    job.update_status(JobStatus.PROCESSING)

    assert job.status == JobStatus.PROCESSING
