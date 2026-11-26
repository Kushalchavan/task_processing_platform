from prometheus_client import Counter, Gauge, Histogram

# -------------------------
# Job lifecycle metrics
# -------------------------

JOBS_CREATED = Counter(
    "nexus_jobs_created_total",
    "Total number of jobs created",
    ["task_type"],
)

JOBS_COMPLETED = Counter(
    "nexus_jobs_completed_total",
    "Total number of jobs completed successfully",
    ["task_type"],
)

JOBS_FAILED = Counter(
    "nexus_jobs_failed_total",
    "Total number of jobs that permanently failed",
    ["task_type"],
)

JOBS_RETRIED = Counter(
    "nexus_jobs_retried_total",
    "Total number of jobs successfully scheduled for retry",
    ["task_type"],
)


# -------------------------
# Job execution metrics
# -------------------------

JOB_PROCESSING_DURATION = Histogram(
    "nexus_job_processing_duration_seconds",
    "Time spent executing job handlers",
    ["task_type"],
    buckets=(0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)

WORKER_JOBS_IN_PROGRESS = Gauge(
    "nexus_worker_jobs_in_progress",
    "Number of jobs currently being executed by this worker process",
)

WORKER_JOBS_PROCESSED = Counter(
    "nexus_worker_jobs_processed_total",
    "Number of job execution attempts by outcome",
    ["task_type", "outcome"],
)
