from job import Job


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