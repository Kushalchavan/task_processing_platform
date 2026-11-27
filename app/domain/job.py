from dataclasses import dataclass, field
from datetime import datetime
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
    JobStatus.PROCESSING: {
        JobStatus.PENDING,
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    },
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
    max_attempts: int = 3
    next_retry_at: datetime | None = None
    last_error: str | None = None

    # Temporary ownership info for crash recovery
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None

    # Prevents an old worker from updating a reclaimed job.
    lease_version: int = 0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

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

    def schedule_retry(
        self,
        error: str,
        retry_at: datetime,
    ) -> bool:
        self.last_error = error

        if self.attempts >= self.max_attempts:
            self.update_status(JobStatus.FAILED)
            self.next_retry_at = None
            return False

        self.update_status(JobStatus.PENDING)
        self.next_retry_at = retry_at
        return True


def create_job(task_type: str, payload: dict[str, Any]) -> Job:
    return Job(task_type=task_type, payload=payload)
