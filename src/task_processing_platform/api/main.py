"""Entry point of the API. Run with: uv run uvicorn src.task_processing_platform.api.main:app --reload"""

from fastapi import FastAPI

from src.task_processing_platform.api.routes import router

app = FastAPI(title="Task Platform")
app.include_router(router)


@app.get("/health")
def health():
    return {"ok": True}
