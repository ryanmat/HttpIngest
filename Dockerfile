# ABOUTME: Multi-stage Dockerfile for LogicMonitor Data Pipeline
# ABOUTME: Uses uv for fast Python dependency management and supports Azure Functions + FastAPI

# Builder stage - install dependencies with uv
FROM python:3.12-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install Python dependencies
RUN uv sync --frozen

# Final stage - Azure Functions runtime
FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv in final stage
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set environment variables for Azure Functions
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \
    FUNCTIONS_WORKER_RUNTIME=python \
    PYTHONUNBUFFERED=1

# Copy dependencies from builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY . /home/site/wwwroot/

WORKDIR /home/site/wwwroot

# Expose ports for both Azure Functions and FastAPI
EXPOSE 7071 8000

# Default command (can be overridden)
CMD ["uv", "run", "python", "-m", "uvicorn", "function_app:fastapi_app", "--host", "0.0.0.0", "--port", "8000"]