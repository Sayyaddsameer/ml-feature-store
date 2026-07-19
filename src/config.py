"""
src/config.py
─────────────
Single source of truth for all runtime configuration.
All values are read exclusively from environment variables via
pydantic-settings — nothing is hardcoded.
"""
from __future__ import annotations

import logging
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    Use `get_settings()` (cached) instead of instantiating directly.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Kafka ────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        ...,
        description="Comma-separated list of Kafka broker addresses, e.g. kafka:29092",
    )
    raw_events_topic: str = Field(
        "raw-events",
        description="Kafka topic name for inbound raw events",
    )
    features_update_topic: str = Field(
        "features-store-updates",
        description="Kafka topic name for outbound processed feature summaries",
    )
    kafka_consumer_group_id: str = Field(
        "feature-store-consumer-group",
        description="Kafka consumer group identifier",
    )
    kafka_consumer_poll_timeout: float = Field(
        1.0,
        ge=0.1,
        description="Seconds to block in consumer.poll()",
    )
    kafka_producer_delivery_timeout_ms: int = Field(
        10_000,
        ge=1_000,
        description="Max milliseconds the producer waits for delivery acknowledgement",
    )

    # ── PostgreSQL ───────────────────────────────────────────────────────
    postgres_host: str = Field(..., description="PostgreSQL hostname")
    postgres_port: int = Field(5432, ge=1, le=65535)
    postgres_db: str = Field(..., description="PostgreSQL database name")
    postgres_user: str = Field(..., description="PostgreSQL user")
    postgres_password: str = Field(..., description="PostgreSQL password")
    postgres_min_connections: int = Field(1, ge=1)
    postgres_max_connections: int = Field(10, ge=1)
    postgres_connect_retry_attempts: int = Field(5, ge=1)
    postgres_connect_retry_base_delay: float = Field(2.0, ge=0.5)

    # ── FastAPI / Uvicorn ────────────────────────────────────────────────
    app_host: str = Field("0.0.0.0")
    app_port: int = Field(8000, ge=1, le=65535)
    log_level: str = Field("INFO")

    # ── Derived helpers ──────────────────────────────────────────────────
    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        level = v.upper()
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in valid:
            raise ValueError(f"log_level must be one of {valid}, got '{v}'")
        return level


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton Settings instance."""
    return Settings()


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structured logging for the whole application.
    Uses a simple, parseable format suitable for log aggregators.
    """
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format=(
            '{"time":"%(asctime)s","level":"%(levelname)s",'
            '"logger":"%(name)s","msg":%(message)s}'
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
