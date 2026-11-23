from unittest.mock import Mock
from app.queue.redis_stream import RedisStream, STREAM_NAME

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


def test_worker_schedules_retry_in_postgres_when_handler_fails(db_session):
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
    assert updated_job.status == JobStatus.PENDING
    assert updated_job.attempts == 1
    assert updated_job.next_retry_at is not None
    assert updated_job.last_error == "Simulated task failure"

    redis_stream.schedule_delayed_job.assert_called_once()

    scheduled_job_id = redis_stream.schedule_delayed_job.call_args.kwargs["job_id"]
    assert scheduled_job_id == job.id


def test_worker_recovers_abandoned_pending_message(db_session):
    repository = PostgresJobRepository(db_session)

    redis_stream = RedisStream()

    # Start with a clean stream and create its consumer group.
    redis_stream.redis.delete(STREAM_NAME)
    redis_stream.create_consumer_group()

    job_service = JobService(repository, redis_stream)

    registry = TaskHandlerRegistry()
    registry.register("print_message", handle_print_message)

    job = job_service.create_job(
        task_type="print_message",
        payload={"message": "Recovered successfully!"},
    )

    # Worker A receives the message but crashes before processing it.
    message = redis_stream.consume_one("worker-1")

    assert message is not None
    assert message["job_id"] == str(job.id)

    # Worker B takes over the abandoned message.
    worker_b = Worker(
        job_service=job_service,
        task_registry=registry,
        redis_stream=redis_stream,
        consumer_name="worker-2",
        recovery_idle_time_ms=0,
    )

    result = worker_b.process_one_job()

    assert result is True

    updated_job = job_service.get_job(job.id)

    assert updated_job is not None
    assert updated_job.status == JobStatus.COMPLETED
    assert updated_job.attempts == 1
