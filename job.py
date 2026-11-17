from typing import Any
from uuid import uuid4


def create_job(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    job = {
        "id": str(uuid4()),
        "task_type": task_type,
        "payload": payload,
        "status": "pending",
        "attempts": 0,
    }

    return job
