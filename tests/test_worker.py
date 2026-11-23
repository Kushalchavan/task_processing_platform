import threading
from unittest.mock import Mock
from datetime import datetime, timedelta, timezone

from app.domain.job import Job, JobStatus
from app.queue.redis_stream import RedisStream
from app.workers.worker import Worker


def create_worker():
    job_service = Mock()
    task_registry = Mock()
    redis_stream = Mock(spec=RedisStream)

    worker = Worker(
        job_service=job_service,
        task_registry=task_registry,
        redis_stream=redis_stream,
        consumer_name="worker-test",
        recovery_idle_time_ms=60000,
    )

    return worker, job_service, task_registry, redis_stream


def test_process_one_job_returns_false_when_no_message():
    worker, _, task_registry, redis_stream = create_worker()

    redis_stream.consume_one.return_value = None
    redis_stream.claim_one_pending.return_value = None

    result = worker.process_one_job()

    assert result is False
    redis_stream.consume_one.assert_called_once_with("worker-test")
    redis_stream.claim_one_pending.assert_called_once_with(
        consumer_name="worker-test",
        min_idle_time=60000,
    )


def test_process_one_job_successfully():
    worker, job_service, task_registry, redis_stream = create_worker()

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus!"},
    )

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": job.id,
    }

    job_service.claim_job.return_value = job

    handler = Mock()
    task_registry.get_handler.return_value = handler

    result = worker.process_one_job()

    assert result is True

    job_service.claim_job.assert_called_once_with(job.id)
    task_registry.get_handler.assert_called_once_with("print_message")
    handler.assert_called_once_with({"message": "Hello Nexus!"})

    job_service.update_job_status.assert_called_once_with(
        job.id,
        JobStatus.COMPLETED,
    )

    redis_stream.acknowledge_job.assert_called_once_with("123-0")


def test_process_one_job_schedules_retry_when_handler_fails():
    worker, job_service, task_registry, redis_stream = create_worker()

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus!"},
    )

    retry_at = datetime.now(timezone.utc) + timedelta(seconds=5)
    updated_job = Job(
        id=job.id,
        task_type=job.task_type,
        payload=job.payload,
        status=JobStatus.PENDING,
        attempts=1,
        next_retry_at=retry_at,
    )

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": job.id,
    }
    job_service.claim_job.return_value = job
    job_service.schedule_job_retry.return_value = (updated_job, True)

    handler = Mock(side_effect=ValueError("Invalid payload"))
    task_registry.get_handler.return_value = handler

    result = worker.process_one_job()

    assert result is True
    job_service.schedule_job_retry.assert_called_once_with(
        job_id=job.id,
        error="Invalid payload",
    )
    redis_stream.schedule_delayed_job.assert_called_once_with(
        job_id=job.id,
        retry_at=retry_at,
    )
    job_service.update_job_status.assert_not_called()
    redis_stream.acknowledge_job.assert_called_once_with("123-0")


def test_message_not_acknowledged_when_job_cannot_be_claimed():
    worker, job_service, _, redis_stream = create_worker()

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": "missing-job",
    }

    job_service.claim_job.return_value = None

    result = worker.process_one_job()

    assert result is True
    redis_stream.acknowledge_job.assert_not_called()


def test_worker_stops_when_shutdown_requested():
    worker, _, _, redis_stream = create_worker()

    worker.shutdown_event.set()
    worker.run()

    redis_stream.consume_one.assert_not_called()


def test_worker_wakes_up_when_shutdown_requested():
    worker, _, _, redis_stream = create_worker()

    redis_stream.consume_one.return_value = None

    # Request shutdown shortly after the worker starts.
    shutdown_thread = threading.Thread(
        target=lambda: (
            threading.Event().wait(0.1),
            worker.shutdown_event.set(),
        )
    )

    shutdown_thread.start()
    worker.run()
    shutdown_thread.join(timeout=1)

    assert not shutdown_thread.is_alive()
    assert redis_stream.consume_one.call_count >= 1


def test_process_one_job_does_not_schedule_retry_when_attempts_exhausted():
    worker, job_service, task_registry, redis_stream = create_worker()

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus!"},
    )

    failed_job = Job(
        id=job.id,
        task_type=job.task_type,
        payload=job.payload,
        status=JobStatus.FAILED,
        attempts=3,
    )

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": job.id,
    }
    job_service.claim_job.return_value = job
    job_service.schedule_job_retry.return_value = (failed_job, False)

    handler = Mock(side_effect=ValueError("Invalid payload"))
    task_registry.get_handler.return_value = handler

    result = worker.process_one_job()

    assert result is True
    job_service.schedule_job_retry.assert_called_once()
    redis_stream.schedule_delayed_job.assert_not_called()
    redis_stream.acknowledge_job.assert_called_once_with("123-0")
