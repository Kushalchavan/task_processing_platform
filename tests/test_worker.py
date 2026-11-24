import threading
import pytest

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
    job.lease_version = 1

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": job.id,
    }

    job_service.claim_job.return_value = job

    handler = Mock()
    task_registry.get_handler.return_value = handler

    result = worker.process_one_job()

    assert result is True

    job_service.claim_job.assert_called_once_with(
        job_id=job.id,
        worker_id="worker-test",
    )
    task_registry.get_handler.assert_called_once_with("print_message")
    handler.assert_called_once_with({"message": "Hello Nexus!"})

    job_service.complete_job.assert_called_once_with(
        job_id=job.id,
        worker_id="worker-test",
        lease_version=1,
    )

    redis_stream.acknowledge_job.assert_called_once_with("123-0")


def test_process_one_job_schedules_retry_when_handler_fails():
    worker, job_service, task_registry, redis_stream = create_worker()

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus!"},
    )
    job.lease_version = 1

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
        worker_id="worker-test",
        lease_version=1,
        error="Invalid payload",
    )

    redis_stream.schedule_delayed_job.assert_called_once_with(
        job_id=job.id,
        retry_at=retry_at,
    )

    job_service.update_job_status.assert_not_called()
    redis_stream.acknowledge_job.assert_called_once_with("123-0")


def test_missing_job_message_is_acknowledged():
    worker, job_service, task_registry, redis_stream = create_worker()

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": "missing-job",
    }
    job_service.claim_job.return_value = None
    job_service.get_job.return_value = None

    result = worker.process_one_job()

    assert result is True
    job_service.get_job.assert_called_once_with("missing-job")
    redis_stream.acknowledge_job.assert_called_once_with("123-0")
    task_registry.get_handler.assert_not_called()


def test_worker_stops_when_shutdown_requested():
    worker, _, _, redis_stream = create_worker()

    worker.shutdown_event.set()
    worker.run()

    redis_stream.consume_one.assert_not_called()


def test_worker_wakes_up_when_shutdown_requested():
    worker, job_service, _, redis_stream = create_worker()

    job_service.recover_expired_jobs.return_value = []
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


def test_completed_job_message_is_acknowledged():
    worker, job_service, task_registry, redis_stream = create_worker()

    completed_job = Mock()
    completed_job.id = "job-123"
    completed_job.status = JobStatus.COMPLETED

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": "job-123",
    }
    job_service.claim_job.return_value = None
    job_service.get_job.return_value = completed_job

    result = worker.process_one_job()

    assert result is True
    redis_stream.acknowledge_job.assert_called_once_with("123-0")
    task_registry.get_handler.assert_not_called()


def test_processing_job_duplicate_message_is_acknowledged():
    worker, job_service, task_registry, redis_stream = create_worker()

    processing_job = Mock()
    processing_job.id = "job-123"
    processing_job.status = JobStatus.PROCESSING

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": "job-123",
    }
    job_service.claim_job.return_value = None
    job_service.get_job.return_value = processing_job

    result = worker.process_one_job()

    assert result is True
    redis_stream.acknowledge_job.assert_called_once_with("123-0")
    task_registry.get_handler.assert_not_called()


def test_future_retry_job_is_rescheduled_and_message_acknowledged():
    worker, job_service, task_registry, redis_stream = create_worker()

    retry_at = datetime.now(timezone.utc) + timedelta(minutes=5)

    pending_job = Mock()
    pending_job.id = "job-123"
    pending_job.status = JobStatus.PENDING
    pending_job.next_retry_at = retry_at

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": "job-123",
    }
    job_service.claim_job.return_value = None
    job_service.get_job.return_value = pending_job

    result = worker.process_one_job()

    assert result is True
    redis_stream.schedule_delayed_job.assert_called_once_with(
        job_id="job-123",
        retry_at=retry_at,
    )
    redis_stream.acknowledge_job.assert_called_once_with("123-0")
    task_registry.get_handler.assert_not_called()


def test_message_not_acknowledged_when_retry_scheduling_fails():
    worker, job_service, task_registry, redis_stream = create_worker()

    retry_at = datetime.now(timezone.utc) + timedelta(minutes=5)

    pending_job = Mock()
    pending_job.id = "job-123"
    pending_job.status = JobStatus.PENDING
    pending_job.next_retry_at = retry_at

    redis_stream.consume_one.return_value = {
        "message_id": "123-0",
        "job_id": "job-123",
    }
    job_service.claim_job.return_value = None
    job_service.get_job.return_value = pending_job

    redis_stream.schedule_delayed_job.side_effect = ConnectionError("Redis unavailable")

    with pytest.raises(ConnectionError):
        worker.process_one_job()

    redis_stream.acknowledge_job.assert_not_called()
    task_registry.get_handler.assert_not_called()
