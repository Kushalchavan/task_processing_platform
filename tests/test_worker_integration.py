from app.repositories.postgres_job_repository import PostgresJobRepository
from app.services.job_service import JobService
from app.workers.task_registry import TaskHandlerRegistry
from app.workers.task_handlers import handle_print_message
from app.workers.worker import Worker
from job import JobStatus


def test_worker_processes_postgres_job(db_session):
    # 1. Connect the service to the real test database
    repository = PostgresJobRepository(db_session)
    job_service = JobService(repository)

    # 2. Create a registry and register the handler
    registry = TaskHandlerRegistry()
    registry.register("print_message", handle_print_message)

    # 3. Create a real job in PostgreSQL
    job = job_service.create_job(
        task_type="print_message",
        payload={"message": "Hello from Task Processing Platform!"},
    )

    assert job.status == JobStatus.PENDING

    # 4. Create the worker
    worker = Worker(job_service, registry)

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
    job_service = JobService(repository)

    registry = TaskHandlerRegistry()

    def failing_handler(payload: dict) -> None:
        raise ValueError("Simulated task failure")

    registry.register("failing_task", failing_handler)

    job = job_service.create_job(
        task_type="failing_task",
        payload={"message": "This should fail"},
    )

    worker = Worker(job_service, registry)

    result = worker.process_one_job()

    assert result is True

    updated_job = job_service.get_job(job.id)

    assert updated_job is not None
    assert updated_job.status == JobStatus.FAILED
    assert updated_job.attempts == 1