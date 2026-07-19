"""
src/consumer.py
───────────────
FeatureConsumer — Kafka consumer that processes raw events into ML features.

Architecture
────────────
* Runs in a daemon background thread so that the FastAPI process stays alive.
* Subscribes to the raw-events topic.
* For every valid message it:
    1. Deserialises JSON → RawEvent (Pydantic validation).
    2. Computes features (see _extract_features).
    3. Upserts all features into PostgreSQL via PostgreSQLManager (idempotent).
    4. Publishes a processed-event summary to features-store-updates topic.
* Malformed / undeserializable messages are logged and skipped (no crash).
* A threading.Event (stop_event) enables clean shutdown from main.py.
* Exponential backoff is applied to transient Kafka poll errors.

Feature Engineering (simple, extensible)
─────────────────────────────────────────
  user_activity_count  — running count of events for the entity (incremented)
  last_action          — event_type of the most-recent event
  last_event_timestamp — ISO-8601 timestamp of the most-recent event
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
from typing import Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError

from src.config import Settings
from src.db_manager import PostgreSQLManager
from src.models import RawEvent

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _jitter_sleep(attempt: int, base: float = 1.0, cap: float = 30.0) -> None:
    """Sleep with full-jitter exponential back-off."""
    ceiling = min(cap, base * (2 ** attempt))
    time.sleep(random.uniform(0, ceiling))


def _delivery_report(err, msg) -> None:
    """Confluent-kafka delivery callback for the internal producer."""
    if err:
        logger.warning(f'"Kafka delivery failed for {msg.topic()}: {err}"')
    else:
        logger.debug(
            f'"Delivered message to {msg.topic()} '
            f'[{msg.partition()}] offset {msg.offset()}"'
        )


# ─────────────────────────────────────────────────────────────────────────────
# Feature extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_features(event: RawEvent, current_count: int) -> Dict[str, str]:
    """
    Derive ML-ready features from a RawEvent.

    Parameters
    ----------
    event         : validated RawEvent
    current_count : the entity's existing activity count (0 if first event)

    Returns
    -------
    dict mapping feature_name → feature_value (all values stored as TEXT)
    """
    new_count = current_count + 1
    features: Dict[str, str] = {
        "user_activity_count":  str(new_count),
        "last_action":          event.event_type,
        "last_event_timestamp": event.timestamp.isoformat(),
    }

    # Optional: enrich from event metadata when present
    if event.metadata:
        if "page" in event.metadata:
            features["last_page_visited"] = str(event.metadata["page"])
        if "amount" in event.metadata:
            features["last_transaction_amount"] = str(event.metadata["amount"])
        if "device" in event.metadata:
            features["last_device"] = str(event.metadata["device"])

    return features


# ─────────────────────────────────────────────────────────────────────────────
# Consumer class
# ─────────────────────────────────────────────────────────────────────────────

class FeatureConsumer:
    """
    Kafka consumer that continuously ingests raw events and materialises
    features into the PostgreSQL feature store.
    """

    def __init__(
        self,
        settings: Settings,
        db_manager: PostgreSQLManager,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self._settings = settings
        self._db = db_manager
        self._stop_event: threading.Event = stop_event or threading.Event()
        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Producer] = None
        self._running: bool = False

    # ── Public interface ─────────────────────────────────────────────────

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> threading.Thread:
        """
        Spin up the consumer in a daemon background thread.
        Returns the Thread so callers can join() it if needed.
        """
        thread = threading.Thread(
            target=self._run_loop,
            name="feature-consumer",
            daemon=True,
        )
        thread.start()
        logger.info('"FeatureConsumer background thread started."')
        return thread

    def stop(self) -> None:
        """Signal the consumer loop to exit gracefully."""
        self._stop_event.set()
        logger.info('"FeatureConsumer stop signal sent."')

    # ── Kafka client setup ───────────────────────────────────────────────

    def _build_consumer(self) -> Consumer:
        config = {
            "bootstrap.servers":        self._settings.kafka_bootstrap_servers,
            "group.id":                 self._settings.kafka_consumer_group_id,
            "auto.offset.reset":        "earliest",
            "enable.auto.commit":       True,
            "auto.commit.interval.ms":  5000,
            "session.timeout.ms":       30_000,
            "heartbeat.interval.ms":    10_000,
            "max.poll.interval.ms":     300_000,
        }
        return Consumer(config)

    def _build_producer(self) -> Producer:
        config = {
            "bootstrap.servers":          self._settings.kafka_bootstrap_servers,
            "delivery.timeout.ms":        self._settings.kafka_producer_delivery_timeout_ms,
            "enable.idempotence":         True,
            "acks":                       "all",
            "retries":                    5,
            "retry.backoff.ms":           200,
        }
        return Producer(config)

    # ── Main loop ────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        """
        Subscribe to the raw-events topic and process messages until
        stop() is called.  Applies exponential back-off on Kafka errors.
        """
        backoff_attempt = 0
        self._running = True

        try:
            self._consumer = self._build_consumer()
            self._producer = self._build_producer()
            self._consumer.subscribe([self._settings.raw_events_topic])
            logger.info(
                f'"Subscribed to topic: {self._settings.raw_events_topic}"'
            )

            while not self._stop_event.is_set():
                try:
                    msg = self._consumer.poll(
                        timeout=self._settings.kafka_consumer_poll_timeout
                    )
                except KafkaException as exc:
                    logger.error(f'"Kafka poll error: {exc}. Backing off…"')
                    _jitter_sleep(backoff_attempt)
                    backoff_attempt = min(backoff_attempt + 1, 6)
                    continue

                if msg is None:
                    # No message in this poll window — reset backoff
                    backoff_attempt = 0
                    continue

                if msg.error():
                    self._handle_kafka_error(msg)
                    continue

                backoff_attempt = 0
                self._process_message(msg)

        except Exception as exc:
            logger.critical(f'"FeatureConsumer crashed: {exc}"', exc_info=True)
        finally:
            self._shutdown_clients()
            self._running = False
            logger.info('"FeatureConsumer loop exited."')

    # ── Message processing ───────────────────────────────────────────────

    def _process_message(self, msg) -> None:
        """
        Deserialise → validate → extract features → store → publish summary.
        Any error is logged and the message is skipped (no crash).
        """
        raw_bytes = msg.value()
        partition = msg.partition()
        offset    = msg.offset()

        # 1. Deserialise JSON
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning(
                f'"Skipping malformed message at partition={partition} '
                f'offset={offset}: {exc}"'
            )
            return

        # 2. Pydantic validation
        try:
            event = RawEvent(**payload)
        except ValidationError as exc:
            logger.warning(
                f'"Skipping invalid event at partition={partition} '
                f'offset={offset}: {exc}"'
            )
            return

        # 3. Retrieve current activity count for incremental feature
        current_count = self._get_current_activity_count(event.entity_id)

        # 4. Extract features
        features = _extract_features(event, current_count)

        # 5. Persist features (idempotent batch upsert)
        try:
            self._db.save_features_batch(event.entity_id, features)
            logger.info(
                f'"Processed event entity_id={event.entity_id} '
                f'event_type={event.event_type} '
                f'features_count={len(features)}"'
            )
        except Exception as exc:
            logger.error(
                f'"Failed to store features for entity_id={event.entity_id}: {exc}"'
            )
            # Do not re-raise — keep the consumer loop alive
            return

        # 6. Publish processed summary to features-store-updates topic
        self._publish_update(event, features)

    def _get_current_activity_count(self, entity_id: str) -> int:
        """
        Look up the current user_activity_count from the feature store.
        Returns 0 if the entity has no stored features yet.
        """
        try:
            records = self._db.get_features(entity_id)
            for record in records:
                if record.feature_name == "user_activity_count":
                    return int(record.feature_value)
        except Exception as exc:
            logger.warning(
                f'"Could not read current count for {entity_id}: {exc}. Defaulting to 0."'
            )
        return 0

    def _publish_update(self, event: RawEvent, features: Dict[str, str]) -> None:
        """Publish a lightweight summary to the features-store-updates topic."""
        if not self._producer:
            return
        summary = {
            "entity_id":      event.entity_id,
            "event_type":     event.event_type,
            "features_saved": list(features.keys()),
            "processed_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        try:
            self._producer.produce(
                topic=self._settings.features_update_topic,
                key=event.entity_id.encode("utf-8"),
                value=json.dumps(summary).encode("utf-8"),
                callback=_delivery_report,
            )
            self._producer.poll(0)  # Trigger delivery callbacks without blocking
        except KafkaException as exc:
            logger.warning(f'"Failed to publish update for {event.entity_id}: {exc}"')

    # ── Error handling ───────────────────────────────────────────────────

    def _handle_kafka_error(self, msg) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            logger.debug(
                f'"Reached end of partition {msg.partition()} '
                f'at offset {msg.offset()}."'
            )
        else:
            logger.error(f'"Kafka message error: {err}"')

    # ── Shutdown ─────────────────────────────────────────────────────────

    def _shutdown_clients(self) -> None:
        if self._producer:
            try:
                # Flush any in-flight messages before closing
                remaining = self._producer.flush(timeout=10)
                if remaining > 0:
                    logger.warning(
                        f'"Producer flushed with {remaining} messages still in queue."'
                    )
            except Exception as exc:
                logger.warning(f'"Producer flush error: {exc}"')
            self._producer = None

        if self._consumer:
            try:
                self._consumer.close()
                logger.info('"Kafka consumer closed gracefully."')
            except Exception as exc:
                logger.warning(f'"Consumer close error: {exc}"')
            self._consumer = None
