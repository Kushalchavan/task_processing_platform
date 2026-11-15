from task_processing_platform.main import create_job


def test_create_job():
    job = create_job(
        "send_email",
        {"email": "user@example.com"}
    )

    assert job["task_type"] == "send_email"
    assert job["status"] == "pending"
    assert job["attempts"] == 0