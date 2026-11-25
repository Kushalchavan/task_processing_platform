# Use Python 3.14 with uv preinstalled
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# Application directory
WORKDIR /app

# Python environment configuration
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency definitions first for better Docker layer caching
COPY pyproject.toml uv.lock ./

# Install production dependencies using the locked versions
RUN uv sync --frozen --no-dev --no-install-project

# Copy application source
COPY app/ ./app/
COPY migrations/ ./migrations/
COPY alembic.ini ./
COPY main.py run_worker.py run_dispatcher.py ./
COPY main.py run_worker.py run_dispatcher.py run_outbox_dispatcher.py ./

# Default process: FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
