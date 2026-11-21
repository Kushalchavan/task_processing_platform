from typing import Any

def handle_print_message(payload: dict[str, Any]) -> None:
    message = payload.get("message")

    if not isinstance(message, str) or not message.strip():
        raise ValueError("A valid message is required")

    print(f"[Task processing worker] {message}")
