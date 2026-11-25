from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database.dependencies import get_db
from app.repositories.postgres_job_repository import PostgresJobRepository
from app.queue.redis_stream import RedisStream
from app.services.job_service import JobService
from app.schemas.job import CreateJob, JobResponse

router = APIRouter(prefix="/jobs", tags=["Jobs"])


def get_job_service(db: Session = Depends(get_db)):
    repository = PostgresJobRepository(db)
    redis_stream = RedisStream()

    return JobService(repository, redis_stream)


@router.post("", response_model=JobResponse, status_code=201)
def create_job(
    request: CreateJob,
    service: JobService = Depends(get_job_service),
):
    return service.create_job(
        task_type=request.task_type,
        payload=request.payload,
    )


@router.get("/{job_id}", response_model=JobResponse)
def get_job(
    job_id: str,
    service: JobService = Depends(get_job_service),
):
    job = service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job does not exist")

    return job


@router.get("", response_model=list[JobResponse])
def get_all_jobs(
    service: JobService = Depends(get_job_service),
):
    return service.get_all_jobs()
