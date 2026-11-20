import threading
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.orm import Session

from app.models.job_model import JobModel
from app.repositories.postgres_job_repository import PostgresJobRepository


def test_two_workers_claim_different_jobs(test_engine):

    job_id_1 = "00000000-0000-0000-0000-000000000001"
    job_id_2 = "00000000-0000-0000-0000-000000000002"

    # Create two pending jobs.
    with Session(test_engine) as db:
        db.add_all(
            [
                JobModel(
                    id=job_id_1,
                    task_type="email",
                    payload={"to": "user1@example.com"},
                    status="pending",
                    attempts=0,
                ),
                JobModel(
                    id=job_id_2,
                    task_type="email",
                    payload={"to": "user2@example.com"},
                    status="pending",
                    attempts=0,
                ),
            ]
        )
        db.commit()

    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def worker_1():
        with Session(test_engine) as db:
            try:
                db.begin()

                job = (
                    db.query(JobModel)
                    .filter(JobModel.id == job_id_1)
                    .with_for_update()
                    .one()
                )

                lock_acquired.set()

                if not release_lock.wait(timeout=10):
                    raise TimeoutError("Worker 1 timed out")

                return job.id

            finally:
                if db.in_transaction():
                    db.rollback()

    def worker_2():
        if not lock_acquired.wait(timeout=10):
            raise TimeoutError("Worker 1 did not acquire the lock")

        with Session(test_engine) as db:
            repository = PostgresJobRepository(db)
            return repository.claim_next_job()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_1 = executor.submit(worker_1)

            assert lock_acquired.wait(timeout=10)

            future_2 = executor.submit(worker_2)

            claimed_job = future_2.result(timeout=10)

            assert claimed_job is not None
            assert claimed_job.id == job_id_2
            assert claimed_job.status.value == "processing"

            release_lock.set()
            assert future_1.result(timeout=10) == job_id_1

    finally:
        release_lock.set()

        with Session(test_engine) as db:
            db.query(JobModel).filter(JobModel.id.in_([job_id_1, job_id_2])).delete(
                synchronize_session=False
            )
            db.commit()
