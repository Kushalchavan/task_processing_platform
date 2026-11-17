from job import Job, JobStatus, create_job as build_job

ALLOWED_TRANSITIONS = {
    JobStatus.PENDING: {JobStatus.PROCESSING},
    JobStatus.PROCESSING: {JobStatus.COMPLETED, JobStatus.FAILED},
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED: set(),
}


class JobManager:
    def __init__(self) -> None:
        self.jobs: dict[str, Job] = {}

    def create_job(self, task_type, payload):
        job = build_job(task_type, payload)

        self.jobs[job.id] = job
        return job

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def get_all_jobs(self):
        return list(self.jobs.values())

    def update_job_status(self, job_id, status):
        job = self.get_job(job_id)

        if job is None:
            raise KeyError(f"Job not found: {job_id}")

        try:
            status = JobStatus(status)
        except ValueError:
            raise ValueError(f"Invalid job status : {status}") from None

        allowed = ALLOWED_TRANSITIONS[job.status]

        if status not in allowed:
            raise ValueError(f"Cannot transition job from " f"{job.status.value} to {status.value}")

        job.status = status

        return job
