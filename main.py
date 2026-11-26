from fastapi import FastAPI

from prometheus_client import make_asgi_app

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

# Expose Prometheus metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
