from fastapi import FastAPI, HTTPException, Depends

from app.repositories.job_repository import JobRepository
from app.services.job_service import JobService
from schema import CreateJob, JobResponse

app = FastAPI()


def get_job_service():
    repository = JobRepository()
    return JobService(repository)


@app.post("/jobs", response_model=JobResponse, status_code=201)
def create_job(request: CreateJob, service: JobService = Depends(get_job_service)):
    return service.create_job(task_type=request.task_type, payload=request.payload)


@app.get("/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: str, service: JobService = Depends(get_job_service)):
    job = service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job does not exist")

    return job


@app.get("/jobs", response_model=list[JobResponse])
def get_all_jobs(service: JobService = Depends(get_job_service)):
    return service.get_all_jobs()
