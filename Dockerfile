# Multi-stage build for smaller image
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies including PostgreSQL client libraries
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY src/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final stage
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Install PostgreSQL client libraries in runtime image
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \
    FUNCTIONS_WORKER_RUNTIME=python \
    FUNCTIONS_CUSTOMHANDLER_PORT=8080 \
    ASPNETCORE_URLS=http://+:8080

# Copy Python packages from builder
COPY --from=builder /root/.local /usr/local

# Copy function code
COPY src/ /home/site/wwwroot/

# Expose port 8080 instead of 80
EXPOSE 8080