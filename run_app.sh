#!/usr/bin/env bash
# =============================================================================
# run_app.sh
# =============================================================================
# Startup script for the feature-client Docker container.
#
# Responsibilities
# ────────────────
#  1. Wait for PostgreSQL to be ready (TCP + pg_isready).
#  2. Wait for Kafka to be ready (TCP).
#  3. Start the FastAPI application via uvicorn.
#
# All configuration is read from environment variables — nothing is hardcoded.
# =============================================================================

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'   # No Colour

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Read config from environment (all mandatory) ────────────────────────────
POSTGRES_HOST="${POSTGRES_HOST:?POSTGRES_HOST is required}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:?POSTGRES_USER is required}"
POSTGRES_DB="${POSTGRES_DB:?POSTGRES_DB is required}"

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
# Extract just the first broker host:port pair for connectivity checks
KAFKA_HOST=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d',' -f1 | cut -d':' -f1)
KAFKA_PORT=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d',' -f1 | cut -d':' -f2)

APP_HOST="${APP_HOST:-0.0.0.0}"
APP_PORT="${APP_PORT:-8000}"
LOG_LEVEL="${LOG_LEVEL:-info}"

MAX_RETRIES=30
RETRY_INTERVAL=2

# ── Wait for TCP port ───────────────────────────────────────────────────────
wait_for_tcp() {
    local host="$1"
    local port="$2"
    local service="$3"
    local attempt=0

    log_info "Waiting for $service at $host:$port …"
    until nc -z "$host" "$port" 2>/dev/null; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge "$MAX_RETRIES" ]; then
            log_error "$service at $host:$port did not become ready after $MAX_RETRIES attempts."
            exit 1
        fi
        log_warn "  $service not ready (attempt $attempt/$MAX_RETRIES). Retrying in ${RETRY_INTERVAL}s…"
        sleep "$RETRY_INTERVAL"
    done
    log_info "$service at $host:$port is reachable ✓"
}

# ── Wait for PostgreSQL (TCP + pg_isready) ──────────────────────────────────
wait_for_postgres() {
    wait_for_tcp "$POSTGRES_HOST" "$POSTGRES_PORT" "PostgreSQL"

    local attempt=0
    log_info "Checking PostgreSQL is accepting queries …"
    until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge "$MAX_RETRIES" ]; then
            log_error "PostgreSQL did not accept queries after $MAX_RETRIES attempts."
            exit 1
        fi
        log_warn "  pg_isready not yet (attempt $attempt/$MAX_RETRIES). Retrying…"
        sleep "$RETRY_INTERVAL"
    done
    log_info "PostgreSQL is ready ✓"
}

# ── Wait for Kafka ──────────────────────────────────────────────────────────
wait_for_kafka() {
    wait_for_tcp "$KAFKA_HOST" "$KAFKA_PORT" "Kafka"
    # Extra grace period — Kafka may still be electing leaders
    log_info "Giving Kafka an extra 5 s to stabilise …"
    sleep 5
    log_info "Kafka is ready ✓"
}

# ── Main sequence ───────────────────────────────────────────────────────────
main() {
    log_info "=== Feature Store Startup ==="
    log_info "PostgreSQL → ${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
    log_info "Kafka      → ${KAFKA_BOOTSTRAP_SERVERS}"
    log_info "App        → ${APP_HOST}:${APP_PORT}  log_level=${LOG_LEVEL}"
    echo

    wait_for_postgres
    wait_for_kafka

    echo
    log_info "=== Starting FastAPI service (uvicorn) ==="
    exec uvicorn src.main:app \
        --host "${APP_HOST}" \
        --port "${APP_PORT}" \
        --log-level "${LOG_LEVEL,,}" \
        --no-access-log
}

main "$@"
