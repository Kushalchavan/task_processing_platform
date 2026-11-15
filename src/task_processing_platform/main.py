def create_job(task_type, payload):
    job = {
        "task_type": task_type,
        "payload": payload,
        "status": "pending",
        "attempts": 0
    }

    return job


if __name__ == "__main__":
    job = create_job(
        task_type="send_email",
        payload={"email": "user@example.com"}
    )

    print(job)