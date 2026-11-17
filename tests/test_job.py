from job import create_job


def test_create_job():
    job = create_job(task_type="send_email", payload={"email": "john@gmail.com"})

    assert job.task_type == "send_email"
    assert job.status == "pending"
    assert job.attempts == 0
    assert job.payload["email"] == "john@gmail.com"
    assert job.id is not None


def test_each_job_has_unique_id():
    job1 = create_job("send_email", {})
    job2 = create_job("send_email", {})

    assert job1.id != job2.id
