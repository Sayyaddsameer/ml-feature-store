"""
src/main.py
───────────
FastAPI application — entry point for the feature-client service.

Startup sequence (via lifespan context manager)
────────────────────────────────────────────────
  1. Configure structured logging.
  2. Initialise PostgreSQLManager (with retry / backoff).
  3. Start FeatureConsumer in a daemon background thread.

Shutdown sequence
─────────────────
  1. Signal FeatureConsumer to stop (sets threading.Event).
  2. Close PostgreSQL connection pool.

Endpoints
─────────
  GET /health                  — liveness + component status check
  GET /features/{entity_id}    — retrieve all features for an entity
"""
from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse

from src.config import configure_logging, get_settings
from src.consumer import FeatureConsumer
from src.db_manager import PostgreSQLManager
from src.models import FeatureResponse, HealthResponse

# ─────────────────────────────────────────────────────────────────────────────
# Module-level singletons (set during lifespan startup)
# ─────────────────────────────────────────────────────────────────────────────

_db_manager: PostgreSQLManager | None = None
_feature_consumer: FeatureConsumer | None = None
_stop_event: threading.Event = threading.Event()

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage startup and graceful shutdown of shared resources."""
    global _db_manager, _feature_consumer

    settings = get_settings()
    configure_logging(settings.log_level)

    logger.info('"Feature Store service starting up…"')

    # 1. Connect to PostgreSQL (with retry)
    _db_manager = PostgreSQLManager(settings)
    _db_manager.connect()

    # 2. Create and start Kafka consumer thread
    _feature_consumer = FeatureConsumer(
        settings=settings,
        db_manager=_db_manager,
        stop_event=_stop_event,
    )
    _feature_consumer.start()

    logger.info('"Feature Store service is ready."')

    yield  # ── Application runs here ──────────────────────────────────────

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info('"Feature Store service shutting down…"')

    if _feature_consumer:
        _feature_consumer.stop()

    if _db_manager:
        _db_manager.close()

    logger.info('"Feature Store service shutdown complete."')


# ─────────────────────────────────────────────────────────────────────────────
# Application
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Event-Driven ML Feature Store",
    description=(
        "Real-time feature ingestion and serving service. "
        "Consumes raw events from Kafka, processes them into ML features, "
        "stores them in PostgreSQL, and exposes a low-latency REST API."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────────────────────
# Dependency helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_db() -> PostgreSQLManager:
    if _db_manager is None or not _db_manager.is_connected:
        raise HTTPException(
            status_code=503,
            detail="Database is not available. Service is starting up or has lost connection.",
        )
    return _db_manager


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    tags=["Observability"],
)
async def health_check() -> HealthResponse:
    """
    Returns the liveness status of the service and its dependencies.

    - `db_connected`            — whether the PostgreSQL pool is open
    - `kafka_consumer_running`  — whether the background consumer thread is alive
    """
    db_ok = _db_manager is not None and _db_manager.is_connected
    consumer_ok = _feature_consumer is not None and _feature_consumer.is_running

    return HealthResponse(
        status="healthy" if (db_ok and consumer_ok) else "degraded",
        db_connected=db_ok,
        kafka_consumer_running=consumer_ok,
        timestamp=datetime.now(tz=timezone.utc),
    )


@app.get(
    "/features/{entity_id}",
    response_model=FeatureResponse,
    summary="Retrieve features for an entity",
    tags=["Features"],
    responses={
        200: {"description": "Features retrieved successfully"},
        404: {"description": "No features found for the given entity_id"},
        503: {"description": "Database unavailable"},
    },
)
async def get_features(
    entity_id: str = Path(
        ...,
        min_length=1,
        max_length=255,
        description="Unique entity identifier (e.g. user_id)",
        example="user_42",
    ),
) -> FeatureResponse:
    """
    Retrieve all materialized features for the specified **entity_id**.

    Features are read directly from the PostgreSQL feature store, which is
    continuously updated by the Kafka consumer.  This endpoint is idempotent
    and safe to call repeatedly with the same entity_id.

    Returns an empty `features` list (HTTP 404) when the entity has not yet
    had any events processed.
    """
    db = _get_db()

    try:
        records = db.get_features(entity_id)
    except Exception as exc:
        logger.error(f'"Error retrieving features for {entity_id}: {exc}"')
        raise HTTPException(
            status_code=503,
            detail="Feature retrieval failed due to a database error.",
        )

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No features found for entity_id='{entity_id}'. "
                   "Ensure events have been produced and processed.",
        )

    logger.info(
        f'"GET /features/{entity_id} → {len(records)} features returned."'
    )
    return FeatureResponse.from_records(entity_id=entity_id, records=records)


# ─────────────────────────────────────────────────────────────────────────────
# Exception handlers
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc: Exception) -> JSONResponse:
    logger.critical(f'"Unhandled exception on {request.url}: {exc}"', exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected internal error occurred."},
    )
