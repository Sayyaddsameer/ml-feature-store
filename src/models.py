"""
src/models.py
─────────────
Pydantic models shared across the application.

  RawEvent         — validates a message consumed from raw-events topic
  FeatureRecord    — represents one row in the features table
  FeatureResponse  — API response wrapper
  HealthResponse   — API health endpoint response
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ── Kafka inbound ─────────────────────────────────────────────────────────────

class RawEvent(BaseModel):
    """
    Schema for a raw event message published to the raw-events Kafka topic.

    All fields are required so that the consumer can safely reject malformed
    messages without crashing the processing loop.
    """

    entity_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique identifier of the entity (e.g. user_id, session_id)",
    )
    event_type: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Type / category of the event (e.g. 'click', 'purchase')",
    )
    timestamp: datetime = Field(
        ...,
        description="ISO-8601 UTC timestamp when the event occurred",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Arbitrary key-value metadata attached to the event",
    )

    @field_validator("entity_id", "event_type", mode="before")
    @classmethod
    def strip_and_lower(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("Must be a string")
        stripped = v.strip()
        if not stripped:
            raise ValueError("Must not be blank")
        return stripped.lower()


# ── Database row ──────────────────────────────────────────────────────────────

class FeatureRecord(BaseModel):
    """One row from the features table."""

    entity_id: str
    feature_name: str
    feature_value: str
    timestamp: datetime

    model_config = {"from_attributes": True}


# ── API response models ───────────────────────────────────────────────────────

class FeatureResponse(BaseModel):
    """Response body for GET /features/{entity_id}."""

    entity_id: str
    features: List[FeatureRecord]
    count: int = Field(description="Total number of feature records returned")

    @classmethod
    def from_records(cls, entity_id: str, records: List[FeatureRecord]) -> "FeatureResponse":
        return cls(entity_id=entity_id, features=records, count=len(records))


class HealthResponse(BaseModel):
    """Response body for GET /health."""

    status: str
    db_connected: bool
    kafka_consumer_running: bool
    timestamp: datetime
