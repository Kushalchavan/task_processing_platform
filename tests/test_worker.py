from unittest.mock import Mock

from job import Job, JobStatus
from app.workers.worker import Worker


def test_process_one_job_returns_false_when_no_job():
    job_service = Mock()
    task_registry = Mock()

    job_service.claim_next_job.return_value = None

    worker = Worker(job_service, task_registry)

    result = worker.process_one_job()

    assert result is False
    task_registry.get_handler.assert_not_called()


def test_process_one_job_successfully():
    job_service = Mock()
    task_registry = Mock()

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus!"},
    )

    handler = Mock()

    job_service.claim_next_job.return_value = job
    task_registry.get_handler.return_value = handler

    worker = Worker(job_service, task_registry)

    result = worker.process_one_job()

    assert result is True

    task_registry.get_handler.assert_called_once_with("print_message")
    handler.assert_called_once_with({"message": "Hello Nexus!"})

    job_service.update_job_status.assert_called_once_with(
        job.id,
        JobStatus.COMPLETED,
    )


def test_process_one_job_marks_failed_when_handler_raises_error():
    job_service = Mock()
    task_registry = Mock()

    job = Job(
        task_type="print_message",
        payload={"message": "Hello Nexus!"},
    )

    handler = Mock(side_effect=ValueError("Invalid payload"))

    job_service.claim_next_job.return_value = job
    task_registry.get_handler.return_value = handler

    worker = Worker(job_service, task_registry)

    result = worker.process_one_job()

    assert result is True

    handler.assert_called_once_with({"message": "Hello Nexus!"})

    job_service.update_job_status.assert_called_once_with(
        job.id,
        JobStatus.FAILED,
    )
