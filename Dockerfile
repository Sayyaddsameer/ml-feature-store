# ════════════════════════════════════════════════════════════════════════════
# Event-Driven ML Feature Store — Dockerfile
# ════════════════════════════════════════════════════════════════════════════
#
# Multi-stage build:
#   Stage 1 (builder) — install Python dependencies into a virtual environment
#   Stage 2 (runtime) — copy only the venv and application code; minimal image
#
# The image is kept small by separating build-time tools from the runtime.
# ════════════════════════════════════════════════════════════════════════════

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Install build tools needed for psycopg2 / confluent-kafka C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install

# Copy only requirements first to maximise Docker layer cache hits
COPY requirements.txt .

# Create an isolated venv to avoid polluting the system Python
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip --no-cache-dir && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


# ── Stage 2: runtime image ───────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Runtime libraries required by psycopg2 and confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        librdkafka1 \
        netcat-openbsd \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the venv from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Ensure venv binaries take precedence on PATH
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy application source code
COPY src/       ./src/
COPY sql/       ./sql/
COPY tests/     ./tests/
COPY producer.py .
COPY run_app.sh .

# Make the startup script executable
RUN chmod +x run_app.sh

# Non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# CMD delegates to run_app.sh which handles wait-for-dependencies + uvicorn
CMD ["./run_app.sh"]
