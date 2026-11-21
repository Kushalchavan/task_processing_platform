from collections.abc import Callable
from typing import Any

TaskHandler = Callable[[dict[str, Any]], None]


class TaskHandlerRegistry:
    def __init__(self) -> None:
        self.handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        self.handlers[task_type] = handler

    def get_handler(self, task_type: str) -> TaskHandler:
        handler = self.handlers.get(task_type)

        if handler is None:
            raise ValueError(f"No handler registered for task: {task_type}")

        return handler
