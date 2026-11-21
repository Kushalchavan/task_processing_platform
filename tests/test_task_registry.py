import pytest

from app.workers.task_registry import TaskHandlerRegistry
from app.workers.task_handlers import handle_print_message


def test_register_handler():
    registry = TaskHandlerRegistry()

    registry.register("print_message", handle_print_message)

    assert registry.handlers["print_message"] is handle_print_message


def test_get_handler():
    registry = TaskHandlerRegistry()

    registry.register("print_message", handle_print_message)

    handler = registry.get_handler("print_message")

    assert handler is handle_print_message


def test_get_handler_raises_error_for_unknown_task():
    registry = TaskHandlerRegistry()

    with pytest.raises(ValueError):
        registry.get_handler("unknown_task")