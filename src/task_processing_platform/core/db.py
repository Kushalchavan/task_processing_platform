"""Every SQL statement lives here, and nowhere else. The API and worker call these functions and never write SQL themselves. If we change the database later, only this file changes"""

import psycopg
from psycopg import Connection
from psycopg.rows import DictRow, dict_row
from psycopg.types.json import Jsonb

from src.task_processing_platform.core.config import DATABASE_URL


def _connect() -> Connection[DictRow]:
    # Connection[DictRow] tells the type checker: rows come back as dicts
    return Connection[DictRow].connect(DATABASE_URL, row_factory=dict_row)


def create_task(task_id: str, task_type: str, payload: dict) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO tasks (id, type, payload, status) VALUES (%s, %s, %s, 'pending')",
            (task_id, task_type, Jsonb(payload)),
        )


def get_task(task_id: str) -> DictRow | None:
    with _connect() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()


def mark_running(task_id: str) -> bool:
    """pending -> running. The `AND status = 'pending'` makes this an atomic claim:
    if another worker already took the task, 0 rows change and we return False.
    This enforces the state machine in the database itself."""
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE tasks SET status = 'running', attempts = attempts + 1, updated_at = now() "
            "WHERE id = %s AND status = 'pending'",
            (task_id,),
        )
        return cur.rowcount == 1


def mark_succeeded(task_id: str, result: dict) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE tasks SET status = 'succeeded', result = %s, updated_at = now() WHERE id = %s",
            (Jsonb(result), task_id),
        )


def mark_failed(task_id: str, error: str) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE tasks SET status = 'failed', error = %s, updated_at = now() WHERE id = %s",
            (error, task_id),
        )
