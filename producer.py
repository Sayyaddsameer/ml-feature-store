"""
producer.py
───────────
Standalone script to simulate raw event production to the Kafka raw-events topic.

Usage
─────
  # Inside Docker:
  docker-compose exec feature-client python producer.py --count 500 --rate 100

  # Locally (with Kafka reachable on localhost:9092):
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer.py --count 200 --rate 50

Environment Variables (override defaults)
─────────────────────────────────────────
  KAFKA_BOOTSTRAP_SERVERS  — broker address(es)  [required]
  RAW_EVENTS_TOPIC         — target topic         [default: raw-events]

CLI Arguments
─────────────
  --count   N    : total number of events to produce  [default: 200]
  --rate    N    : target events per second            [default: 100]
  --entity-count : number of distinct entity_ids       [default: 20]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("producer")

# ─── Constants ────────────────────────────────────────────────────────────────
EVENT_TYPES = [
    "click", "view", "purchase", "search",
    "add_to_cart", "remove_from_cart", "checkout",
    "login", "logout", "page_view",
]
PAGES = [
    "home", "product_list", "product_detail",
    "cart", "checkout", "profile", "search_results",
]
DEVICES = ["mobile", "desktop", "tablet"]


# ─── Delivery callback ────────────────────────────────────────────────────────
def _delivery_report(err, msg) -> None:
    if err:
        logger.warning(f'"Delivery failed: {err}"')
    else:
        logger.debug(
            f'"Delivered → {msg.topic()} [{msg.partition()}] @ {msg.offset()}"'
        )


# ─── Event factory ────────────────────────────────────────────────────────────
def _make_event(entity_id: str) -> dict:
    event_type = random.choice(EVENT_TYPES)
    metadata: dict = {
        "page":   random.choice(PAGES),
        "device": random.choice(DEVICES),
    }
    if event_type == "purchase":
        metadata["amount"] = round(random.uniform(5.0, 500.0), 2)

    return {
        "entity_id":  entity_id,
        "event_type": event_type,
        "timestamp":  datetime.now(tz=timezone.utc).isoformat(),
        "metadata":   metadata,
    }


# ─── Producer setup ──────────────────────────────────────────────────────────
def _build_producer(bootstrap_servers: str) -> Producer:
    config = {
        "bootstrap.servers":   bootstrap_servers,
        "acks":                "all",
        "retries":             5,
        "retry.backoff.ms":    200,
        "delivery.timeout.ms": 10_000,
    }
    return Producer(config)


# ─── Main ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka raw-event producer for the ML Feature Store.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=200,
        help="Total number of events to produce (default: 200)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=100.0,
        help="Target events per second (default: 100)",
    )
    parser.add_argument(
        "--entity-count",
        type=int,
        default=20,
        help="Number of distinct entity_ids to simulate (default: 20)",
    )
    args = parser.parse_args()

    # Read broker config from environment — no hardcoding
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    if not bootstrap_servers:
        logger.error('"KAFKA_BOOTSTRAP_SERVERS environment variable is not set."')
        sys.exit(1)

    topic = os.environ.get("RAW_EVENTS_TOPIC", "raw-events")
    entity_ids = [f"user_{i:04d}" for i in range(1, args.entity_count + 1)]

    logger.info(
        f'"Starting producer: count={args.count} rate={args.rate}/s '
        f'entities={args.entity_count} topic={topic} broker={bootstrap_servers}"'
    )

    producer = _build_producer(bootstrap_servers)
    interval = 1.0 / args.rate  # seconds between events
    sent = 0
    start_time = time.monotonic()

    for i in range(args.count):
        entity_id = random.choice(entity_ids)
        event = _make_event(entity_id)

        try:
            producer.produce(
                topic=topic,
                key=entity_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=_delivery_report,
            )
        except KafkaException as exc:
            logger.error(f'"Produce error: {exc}"')
            continue

        sent += 1

        # Throttle to maintain target rate
        elapsed = time.monotonic() - start_time
        expected = (i + 1) * interval
        sleep_for = expected - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

        # Periodically flush to trigger delivery callbacks
        if sent % 50 == 0:
            producer.poll(0)
            logger.info(f'"Progress: {sent}/{args.count} events produced."')

    # Final flush — wait up to 30 s for all outstanding messages
    remaining = producer.flush(30)
    elapsed = time.monotonic() - start_time
    throughput = sent / elapsed if elapsed > 0 else 0

    logger.info(
        f'"Finished: {sent} events in {elapsed:.2f}s '
        f'({throughput:.1f} events/s). '
        f'Outstanding messages: {remaining}."'
    )


if __name__ == "__main__":
    main()
