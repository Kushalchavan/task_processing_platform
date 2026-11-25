from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from app.queue.redis_stream import RedisStream, STREAM_NAME
from app.repositories.postgres_job_repository import PostgresJobRepository
from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.workers.task_handlers import handle_print_message
from app.workers.worker import Worker
from app.domain.job import JobStatus
from app.queue.delayed_job_dispatcher import DelayedJobDispatcher


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


def test_worker_recovers_abandoned_pending_message(db_session, clean_test_redis):
    repository = PostgresJobRepository(db_session)
    redis_stream = RedisStream(clean_test_redis)

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


def test_worker_retries_failed_job_successfully(db_session, clean_test_redis):
    repository = PostgresJobRepository(db_session)
    redis_stream = RedisStream(clean_test_redis)

    # Keep this test isolated from Redis data left by other tests.
    redis_stream.redis.delete(STREAM_NAME)
    redis_stream.redis.delete("nexus:delayed_jobs")
    redis_stream.create_consumer_group()

    job_service = JobService(repository, redis_stream)

    registry = TaskHandlerRegistry()

    execution_count = 0

    def retry_handler(payload: dict) -> None:
        nonlocal execution_count
        execution_count += 1

        # Simulate a temporary failure on the first attempt.
        if execution_count == 1:
            raise ValueError("Temporary failure")

    registry.register("retry_task", retry_handler)

    job = job_service.create_job(
        task_type="retry_task",
        payload={"message": "Retry me"},
    )

    worker = Worker(
        job_service=job_service,
        task_registry=registry,
        redis_stream=redis_stream,
        consumer_name="worker-test",
    )

    # First attempt: the handler fails and schedules a delayed retry.
    assert worker.process_one_job() is True

    updated_job = job_service.get_job(job.id)

    assert updated_job is not None
    assert updated_job.status == JobStatus.PENDING
    assert updated_job.attempts == 1
    assert updated_job.next_retry_at is not None

    # Make the retry due in both PostgreSQL and Redis.
    retry_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    # Update the retry timestamp in PostgreSQL.
    updated_job.next_retry_at = retry_at
    repository.save(updated_job)
    repository.commit()

    # Update the retry timestamp in Redis.
    redis_stream.schedule_delayed_job(
        job_id=job.id,
        retry_at=retry_at,
    )

    # The dispatcher moves the due job into the Redis Stream.
    dispatcher = DelayedJobDispatcher(
        redis_stream=redis_stream,
        poll_interval=0.1,
        batch_size=10,
    )

    dispatched_count = dispatcher.dispatch_once()
    assert dispatched_count == 1

    # Second attempt: the handler succeeds.
    assert worker.process_one_job() is True

    completed_job = job_service.get_job(job.id)

    assert completed_job is not None
    assert completed_job.status == JobStatus.COMPLETED
    assert completed_job.attempts == 2
    assert execution_count == 2


def test_worker_recovers_and_executes_expired_postgres_lease(db_session):
    repository = PostgresJobRepository(db_session)
    redis_stream = Mock(spec=RedisStream)
    job_service = JobService(repository, redis_stream)

    registry = TaskHandlerRegistry()

    execution_count = 0

    def recovery_handler(payload: dict) -> None:
        nonlocal execution_count
        execution_count += 1

    registry.register("recovery_task", recovery_handler)

    # Create a job in PostgreSQL.
    job = job_service.create_job(
        task_type="recovery_task",
        payload={"message": "Recover this job"},
    )

    # Worker A claims the job but crashes before executing it.
    claimed_job = job_service.claim_job(
        job_id=str(job.id),
        worker_id="worker-A",
    )

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.lease_owner == "worker-A"
    assert claimed_job.attempts == 1

    # Simulate Worker A's lease expiring.
    claimed_job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    repository.save(claimed_job)
    repository.commit()

    # Worker B recovers and executes the expired job.
    worker_b = Worker(
        job_service=job_service,
        task_registry=registry,
        redis_stream=redis_stream,
        consumer_name="worker-B",
    )

    recovered_count = worker_b.recover_expired_jobs()

    # Verify recovery and execution.
    assert recovered_count == 1
    assert execution_count == 1

    completed_job = job_service.get_job(job.id)

    assert completed_job is not None
    assert completed_job.status == JobStatus.COMPLETED
    assert completed_job.attempts == 2
    assert completed_job.lease_owner is None
    assert completed_job.lease_expires_at is None

    # Recovery executes the already-claimed job directly.
    # It does not consume or claim another Redis message.
    redis_stream.consume_one.assert_not_called()
    redis_stream.claim_one_pending.assert_not_called()
