from app.domain.job import Job, JobStatus


class JobRepository:

    def __init__(self):
        self.jobs: dict[str, Job] = {}

    def create(self, job: Job) -> Job:
        self.jobs[job.id] = job
        return job

    def get_by_id(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def get_all(self) -> list[Job]:
        return list(self.jobs.values())

    def save(self, job: Job) -> Job:
        self.jobs[job.id] = job
        return job

    def claim_next_job(self) -> Job | None:
        for job in self.jobs.values():
            if job.status == JobStatus.PENDING:
                job.update_status(JobStatus.PROCESSING)
                job.attempts += 1
                return job

        return None

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass
