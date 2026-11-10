"""HTTP only: receive a request, call core functions, return a response."""

from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException

from src.task_processing_platform.core import db, queue
from src.task_processing_platform.core.models import TaskCreated, TaskIn

router = APIRouter()


@router.post("/tasks", response_model=TaskCreated, status_code=202)
def submit_task(task: TaskIn):
    task_id = str(uuid4())
    # ORDER MATTERS: save to Postgres first, then enqueue. That way a task in
    # the queue always has a row in the DB. (The reverse gap, saved but not
    # enqueued if we crash between the two lines, is a v2 problem: the reaper.)
    db.create_task(task_id, task.type, task.payload)
    queue.enqueue(task_id)
    return TaskCreated(
        task_id=task_id, status="pending"
    )  # 202 = accepted, not done yet


@router.get("/tasks/{task_id}")
def get_task(task_id: UUID):  # FastAPI rejects malformed IDs with a 422 automatically
    task = db.get_task(str(task_id))
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return task
