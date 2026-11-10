"""The worker: a loop that takes a task ID from the queue, runs the handler, and records the outcome. Run with: uv run python -m app.worker.worker Start as many copies as you like, in separate terminals."""

import signal

from src.task_processing_platform.core import db, queue
from src.task_processing_platform.worker.handlers import HANDLERS

running = True


def _stop(signum, frame):
    # Graceful shutdown: finish the current task, then exit the loop.
    global running
    print("shutdown requested, finishing current task...")
    running = False


def process(task_id: str) -> None:
    task = db.get_task(task_id)
    if task is None:
        print(f"[{task_id}] not in DB, skipping")
        return
    if not db.mark_running(task_id):
        print(f"[{task_id}] already claimed by someone else, skipping")
        return

    print(f"[{task_id}] running {task['type']}")
    try:
        handler = HANDLERS.get(task["type"])
        if handler is None:
            raise ValueError(f"unknown task type: {task['type']}")
        result = handler(task["payload"])
        db.mark_succeeded(task_id, result)
        print(f"[{task_id}] succeeded")
    except Exception as exc:
        db.mark_failed(task_id, str(exc))
        print(f"[{task_id}] failed: {exc}")


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    print("worker started, waiting for tasks...")
    while running:
        task_id = queue.dequeue(timeout=5)
        if task_id:
            process(task_id)
    print("worker stopped")


if __name__ == "__main__":
    main()
