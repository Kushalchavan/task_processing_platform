from pydantic import BaseModel, Field
from typing import Any

class CreateJob(BaseModel):
    task_type: str = Field(min_length = 1)
    payload: dict[str, Any]

class JobResponse(BaseModel):
    id : str
    task_type: str
    payload: dict[str, Any]
    status: str
    attempts: int