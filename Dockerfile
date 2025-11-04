# Multi-stage build for smaller image
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \
    FUNCTIONS_WORKER_RUNTIME=python \
    FUNCTIONS_CUSTOMHANDLER_PORT=8080 \
    ASPNETCORE_URLS=http://+:8080

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy function code
COPY src/ /home/site/wwwroot/

# Install requirements again to ensure Azure Functions finds them
WORKDIR /home/site/wwwroot
RUN pip install --no-cache-dir -r requirements.txt

# REMOVED: User creation and USER directive - Azure Functions needs root
# RUN useradd -m -u 1000 appuser && \
#     chown -R appuser:appuser /home/site/wwwroot
# USER appuser

# Expose the correct port
EXPOSE 8080