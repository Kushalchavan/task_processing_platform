from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


ALLOWED_TRANSITIONS = {
    JobStatus.PENDING: {JobStatus.PROCESSING},
    JobStatus.PROCESSING: {JobStatus.COMPLETED, JobStatus.FAILED},
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED: set(),
}


@dataclass
class Job:
    task_type: str
    payload: dict[str, Any]
    id: str = field(default_factory=lambda: str(uuid4()))
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0

    def update_status(self, status: JobStatus | str) -> None:
        try:
            status = JobStatus(status)
        except ValueError:
            raise ValueError(f"Invalid job status: {status}") from None

        allowed = ALLOWED_TRANSITIONS[self.status]

        if status not in allowed:
            raise ValueError(
                f"Cannot transition job from " f"{self.status.value} to {status.value}"
            )

        self.status = status


def create_job(task_type: str, payload: dict[str, Any]) -> Job:
    return Job(task_type=task_type, payload=payload)
