"""Shapes of data crossing the HTTP boundary. Pydantic validates incoming JSON automatically: a bad request gets a 422 before your code runs."""

from pydantic import BaseModel, Field


class TaskIn(BaseModel):
    type: str  # example: "send_email"
    payload: dict = Field(default_factory=dict)  # input for the handler


class TaskCreated(BaseModel):
    task_id: str
    status: str
