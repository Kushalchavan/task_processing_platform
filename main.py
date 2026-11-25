from fastapi import FastAPI

from app.api.routes.jobs import (
    get_job_service,
    router as jobs_router,
)

app = FastAPI(
    title="Nexus",
    description="Distributed Task Processing Platform",
    version="1.0.0",
)

app.include_router(jobs_router)
