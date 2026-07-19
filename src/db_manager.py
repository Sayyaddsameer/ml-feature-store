"""
src/db_manager.py
─────────────────
PostgreSQLManager — all database interaction lives here.

Design goals
────────────
* Connection pooling via psycopg2.pool.ThreadedConnectionPool so that the
  FastAPI main thread AND the consumer background thread share the same pool
  safely.
* Idempotent writes using INSERT … ON CONFLICT DO UPDATE (UPSERT).
* Exponential backoff with jitter for transient connection failures.
* Explicit error logging so that callers never receive silent failures.
"""
from __future__ import annotations

import logging
import random
import time
from contextlib import contextmanager
from typing import Generator, List, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

from src.config import Settings
from src.models import FeatureRecord

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Retry helper
# ─────────────────────────────────────────────────────────────────────────────

def _exponential_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """
    Return a sleep duration with full-jitter exponential back-off.
    Formula: random.uniform(0, min(cap, base * 2 ** attempt))
    """
    ceiling = min(cap, base * (2 ** attempt))
    return random.uniform(0, ceiling)


# ─────────────────────────────────────────────────────────────────────────────
# Manager class
# ─────────────────────────────────────────────────────────────────────────────

class PostgreSQLManager:
    """
    Thread-safe PostgreSQL manager backed by a connection pool.

    Usage
    ─────
    manager = PostgreSQLManager(settings)
    manager.connect()           # call once at startup
    manager.save_feature(...)
    records = manager.get_features(entity_id)
    manager.close()             # call at shutdown
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

    # ── Lifecycle ────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Initialise the connection pool with exponential backoff.
        Raises RuntimeError after all retry attempts are exhausted.
        """
        attempts = self._settings.postgres_connect_retry_attempts
        base_delay = self._settings.postgres_connect_retry_base_delay

        for attempt in range(attempts):
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self._settings.postgres_min_connections,
                    maxconn=self._settings.postgres_max_connections,
                    dsn=self._settings.postgres_dsn,
                )
                logger.info(
                    '"Successfully connected to PostgreSQL. '
                    f'Pool size: {self._settings.postgres_min_connections}–'
                    f'{self._settings.postgres_max_connections}"'
                )
                return
            except psycopg2.OperationalError as exc:
                wait = _exponential_backoff(attempt, base_delay)
                logger.warning(
                    f'"PostgreSQL connection attempt {attempt + 1}/{attempts} failed: '
                    f'{exc}. Retrying in {wait:.1f}s…"'
                )
                time.sleep(wait)

        raise RuntimeError(
            f"Could not connect to PostgreSQL after {attempts} attempts. "
            "Check POSTGRES_HOST, POSTGRES_PORT, and credentials."
        )

    def close(self) -> None:
        """Close all pooled connections."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info('"PostgreSQL connection pool closed."')

    @property
    def is_connected(self) -> bool:
        return self._pool is not None

    # ── Context manager for connections ──────────────────────────────────

    @contextmanager
    def _get_connection(self) -> Generator:
        """
        Yield a pooled connection and auto-return it when done.
        Rolls back on exception.
        """
        if not self._pool:
            raise RuntimeError("PostgreSQLManager is not connected. Call connect() first.")
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    # ── Write operations ─────────────────────────────────────────────────

    def save_feature(
        self,
        entity_id: str,
        feature_name: str,
        feature_value: str,
    ) -> None:
        """
        Upsert a single feature for an entity.

        Uses INSERT … ON CONFLICT DO UPDATE so that re-processing the same
        Kafka message is fully idempotent — only the timestamp and value
        are refreshed when a duplicate key is detected.
        """
        sql = """
            INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (entity_id, feature_name)
            DO UPDATE SET
                feature_value = EXCLUDED.feature_value,
                timestamp     = EXCLUDED.timestamp;
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (entity_id, feature_name, str(feature_value)))
            logger.debug(
                f'"Saved feature entity_id={entity_id} '
                f'feature_name={feature_name} '
                f'feature_value={feature_value}"'
            )
        except psycopg2.Error as exc:
            logger.error(
                f'"Failed to save feature entity_id={entity_id} '
                f'feature_name={feature_name}: {exc}"'
            )
            raise

    def save_features_batch(
        self,
        entity_id: str,
        features: dict,
    ) -> None:
        """
        Upsert multiple features for an entity in a single transaction.
        More efficient than calling save_feature() repeatedly.
        """
        sql = """
            INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
            VALUES %s
            ON CONFLICT (entity_id, feature_name)
            DO UPDATE SET
                feature_value = EXCLUDED.feature_value,
                timestamp     = EXCLUDED.timestamp;
        """
        rows = [(entity_id, name, str(value)) for name, value in features.items()]
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, rows)
            logger.debug(
                f'"Saved {len(rows)} features in batch for entity_id={entity_id}"'
            )
        except psycopg2.Error as exc:
            logger.error(
                f'"Failed batch save for entity_id={entity_id}: {exc}"'
            )
            raise

    # ── Read operations ──────────────────────────────────────────────────

    def get_features(self, entity_id: str) -> List[FeatureRecord]:
        """
        Retrieve all features for a given entity_id.
        Returns an empty list if the entity is unknown.
        """
        sql = """
            SELECT entity_id, feature_name, feature_value, timestamp
            FROM   features
            WHERE  entity_id = %s
            ORDER  BY feature_name;
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, (entity_id,))
                    rows = cur.fetchall()
            return [FeatureRecord(**row) for row in rows]
        except psycopg2.Error as exc:
            logger.error(
                f'"Failed to retrieve features for entity_id={entity_id}: {exc}"'
            )
            raise
