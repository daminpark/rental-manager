ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.21
FROM ${BUILD_FROM}

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache gcc musl-dev

# Copy project files
COPY pyproject.toml ./
COPY src ./src

# Install Python dependencies
RUN pip install --no-cache-dir .

# Create data directory for SQLite database
RUN mkdir -p /data

# Copy run script for s6-overlay
COPY run.sh /etc/services.d/rental_manager/run
RUN chmod +x /etc/services.d/rental_manager/run
