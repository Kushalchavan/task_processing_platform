# Task Platform (v1)

A distributed task queue: API -> Redis -> workers -> Postgres.

## Run

```bash
docker compose up -d                       # Postgres + Redis
uv sync                                    # install dependencies
uv run uvicorn src.task_processing_platform.api.main:app --reload   # terminal 1: API
uv run python -m src.task_processing_platform.worker.worker         # terminal 2: worker (start more for more workers)
```

## Try it

```bash
curl -X POST localhost:8000/tasks -H "Content-Type: application/json" \
  -d '{"type": "send_email", "payload": {"to": "a@b.com"}}'
curl localhost:8000/tasks/<task_id>
```

Interactive docs: http://localhost:8000/docs
Reset the database: `docker compose down -v && docker compose up -d`

## Known limitations (v1 -> v2 work)
- Worker crash after popping = lost task
- Crash between DB save and enqueue = task never queued
- No retries, backoff, or dead-letter state
- Failed tasks stay `failed` forever