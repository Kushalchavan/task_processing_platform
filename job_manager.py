from job import create_job as build_job


class JobManager:
    def __init__(self) -> None:
        self.jobs = {}

    def create_job(self, task_type, payload):
        job = build_job(task_type, payload)

        self.jobs[job["id"]] = job
        return job

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def get_all_jobs(self):
        return list(self.jobs.values())

    def update_job_status(self, job_id, status):
        job = self.get_job(job_id)

        if job is None:
            raise KeyError(f"Job not found", {job_id})

        valid_statuses = {"pending", "processing", "completed", "failed"}

        if status not in valid_statuses:
            raise ValueError(f"Invalid job status", {status})

        job["status"] = status

        return job

