from unittest.mock import Mock
from app.queue.redis_stream import RedisStream

from app.repositories.postgres_job_repository import PostgresJobRepository
from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.workers.task_handlers import handle_print_message
from app.workers.worker import Worker
from app.domain.job import JobStatus


def test_worker_processes_postgres_job(db_session):
    # 1. Connect the service to the real test database
    repository = PostgresJobRepository(db_session)
    redis_stream = Mock(spec=RedisStream)
    job_service = JobService(repository, redis_stream)
    # 2. Create a registry and register the handler
    registry = TaskHandlerRegistry()
    registry.register("print_message", handle_print_message)

    # 3. Create a real job in PostgreSQL
    job = job_service.create_job(
        task_type="print_message",
        payload={"message": "Hello from Task Processing Platform!"},
    )

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": str(job.id),
    }

    assert job.status == JobStatus.PENDING

    # 4. Create the worker
    worker = Worker(
        job_service=job_service,
        task_registry=registry,
        redis_stream=redis_stream,
        consumer_name="worker-test",
    )

    # 5. Process the job
    result = worker.process_one_job()

    # 6. Verify execution
    assert result is True

    # 7. Verify the persisted status
    updated_job = job_service.get_job(job.id)

    assert updated_job is not None
    assert updated_job.status == JobStatus.COMPLETED
    assert updated_job.attempts == 1


def test_worker_marks_postgres_job_failed(db_session):
    repository = PostgresJobRepository(db_session)
    redis_stream = Mock(spec=RedisStream)
    job_service = JobService(repository, redis_stream)

    registry = TaskHandlerRegistry()

    def failing_handler(payload: dict) -> None:
        raise ValueError("Simulated task failure")

    registry.register("failing_task", failing_handler)

    job = job_service.create_job(
        task_type="failing_task",
        payload={"message": "This should fail"},
    )

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": str(job.id),
    }

    worker = Worker(
        job_service=job_service,
        task_registry=registry,
        redis_stream=redis_stream,
        consumer_name="worker-test",
    )

    result = worker.process_one_job()

    assert result is True

    updated_job = job_service.get_job(job.id)

    assert updated_job is not None
    assert updated_job.status == JobStatus.FAILED
    assert updated_job.attempts == 1

    redis_stream.acknowledge_job.assert_called_once_with("123-0")
