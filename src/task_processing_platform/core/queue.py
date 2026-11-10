"""Everything that touches Redis lives here. Note we put only the task ID in the queue, not the whole task. Postgres is the source of truth; Redis is just a fast 'you have work to do' signal."""

import redis

from src.task_processing_platform.core.config import QUEUE_NAME, REDIS_URL

_r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def enqueue(task_id: str) -> None:
    _r.lpush(QUEUE_NAME, task_id)  # push on the left...


def dequeue(timeout: int = 5) -> str | None:
    """Block up to `timeout` seconds waiting for a task ID (taken from the right, so the queue is(first-in-first-out). Returns None if nothing arrived. The timeout lets the worker loop wake up regularly to check for shutdown."""
    item = _r.brpop(QUEUE_NAME, timeout=timeout)
    return item[1] if item else None