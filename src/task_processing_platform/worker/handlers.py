"""The actual work. One function per task type. Each takes the payload dict and returns a dict (the result). To add a task type: write a function and register it in HANDLERS. Nothing else changes."""

import time


def send_email(payload: dict) -> dict:
    time.sleep(1)  # pretend to talk to an email server
    return {"sent_to": payload.get("to", "unknown")}


def sleep(payload: dict) -> dict:
    seconds = float(payload.get("seconds", 2))
    time.sleep(seconds)  # useful for testing: a long task you can kill mid-way
    return {"slept": seconds}


def always_fail(payload: dict) -> dict:
    raise RuntimeError("this handler always fails (for testing failures)")


HANDLERS = {
    "send_email": send_email,
    "sleep": sleep,
    "always_fail": always_fail,
}
