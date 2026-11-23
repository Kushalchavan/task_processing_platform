from collections.abc import Generator

from sqlalchemy.orm import Session

from app.database.connection import SessionLocal
from app.repositories.postgres_job_repository import PostgresJobRepository
from app.queue.redis_stream import RedisStream
from app.services.job_service import JobService


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


def get_job_service():
    db = SessionLocal()

    try:
        repository = PostgresJobRepository(db)
        redis_stream = RedisStream()

        yield JobService(repository, redis_stream)
    finally:
        db.close()
