# Multi-stage build for production optimization
FROM python:3.9-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY main.py .
COPY config/ ./config/

# Create non-root user for security
RUN groupadd -r etluser && useradd -r -g etluser etluser
RUN chown -R etluser:etluser /app
USER etluser

# Default command
CMD ["python", "main.py"]

# Development stage
FROM base as dev
USER root
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
USER etluser

# Enable hot reload for development
ENV PYTHONPATH=/app/src
CMD ["python", "-m", "watchdog", "--patterns=*.py", "--command=python main.py", "src/"]