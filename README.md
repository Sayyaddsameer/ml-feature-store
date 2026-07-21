# 🚀 Event-Driven ML Feature Store Client

A production-grade, event-driven machine learning feature store built with **Apache Kafka**, **PostgreSQL**, and **FastAPI**. Raw data events are ingested in real time, transformed into ML features, and served via a low-latency REST API — all containerised and ready to run with a single command.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Running the Producer](#running-the-producer)
- [API Reference](#api-reference)
- [API Screenshots](#api-screenshots)
- [Configuration](#configuration)
- [Design Decisions](#design-decisions)
- [Error Handling & Resilience](#error-handling--resilience)
- [Running Tests](#running-tests)
- [Postman Collection](#postman-collection)
- [Performance](#performance)

---

## Overview

Traditional batch-processed feature stores suffer from stale features at inference time. This project demonstrates an **event-driven** alternative:

```
Raw Events (Kafka)  →  Feature Consumer  →  PostgreSQL Feature Store  →  FastAPI  →  ML Models
```

| Component | Technology | Role |
|---|---|---|
| Message broker | Apache Kafka | Decoupled, durable event transport |
| Feature processing | Python (confluent-kafka) | Consume, validate, transform events |
| Feature store | PostgreSQL | Persistent, indexed feature storage |
| Serving API | FastAPI + uvicorn | Low-latency feature retrieval |
| Containerisation | Docker + Docker Compose | One-command deployment |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Docker Network (feature-net)                    │
│                                                                          │
│  ┌─────────────┐    raw-events     ┌──────────────────────────────────┐ │
│  │  producer.py│ ────────────────► │       feature-client             │ │
│  │  (external) │                   │  ┌──────────────────────────────┐│ │
│  └─────────────┘                   │  │  FastAPI (main thread)       ││ │
│                                    │  │  GET /health                 ││ │
│  ┌─────────────┐                   │  │  GET /features/{entity_id}   ││ │
│  │  Zookeeper  │◄──────────────────│  └─────────────────┬────────────┘│ │
│  └──────┬──────┘                   │                     │ reads       │ │
│         │                          │  ┌──────────────────▼────────────┐│ │
│  ┌──────▼──────┐                   │  │  PostgreSQLManager (pool)     ││ │
│  │    Kafka    │◄─────────────────►│  └──────────────────▲────────────┘│ │
│  │  Broker     │   features-store- │                     │ writes      │ │
│  └─────────────┘   updates         │  ┌──────────────────┴────────────┐│ │
│                                    │  │  FeatureConsumer (bg thread)  ││ │
│  ┌─────────────┐                   │  │  - poll raw-events            ││ │
│  │  PostgreSQL │◄──────────────────│  │  - validate (Pydantic)        ││ │
│  │  (features) │                   │  │  - extract features           ││ │
│  └─────────────┘                   │  │  - upsert to DB               ││ │
│                                    │  └───────────────────────────────┘│ │
│                                    └──────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

### Data Flow (Sequence)

```
producer.py          Kafka (raw-events)    FeatureConsumer       PostgreSQL        FastAPI
     │                      │                    │                   │                │
     │──── produce() ──────►│                    │                   │                │
     │                      │──── poll() ────────►│                   │                │
     │                      │                    │── validate JSON ──►│                │
     │                      │                    │── extract feats ──►│                │
     │                      │                    │── UPSERT batch ───►│                │
     │                      │◄── publish ────────│                   │                │
     │               (features-store-updates)    │                   │                │
     │                                                               │                │
     │                                                               │◄── GET /feats ─│
     │                                                               │─── SELECT ─────►│
     │                                                               │◄── rows ────────│
     │                                                               │──── JSON ───────►
```

---

## Project Structure

```
ml-feature-store/
├── .env.example              # All required environment variables
├── docker-compose.yml        # Orchestrates Kafka, ZooKeeper, PostgreSQL, feature-client
├── Dockerfile                # Multi-stage build for feature-client service
├── producer.py               # Standalone script to simulate raw event production
├── run_app.sh                # Container startup: waits for deps, launches uvicorn
├── requirements.txt          # Pinned Python dependencies
├── README.md                 # This file
├── sql/
│   └── init.sql              # PostgreSQL schema (features table + index)
├── src/
│   ├── __init__.py
│   ├── config.py             # pydantic-settings — all config from env vars
│   ├── models.py             # Pydantic models (RawEvent, FeatureRecord, etc.)
│   ├── db_manager.py         # PostgreSQLManager — connection pool, UPSERT, SELECT
│   ├── consumer.py           # FeatureConsumer — Kafka → features → DB
│   └── main.py               # FastAPI app + lifespan startup/shutdown
└── tests/
    ├── __init__.py
    └── test_main.py          # Unit + integration tests (no real Kafka/DB needed)
```

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) ≥ 20.10
- [Docker Compose](https://docs.docker.com/compose/install/) ≥ 1.29

### 1 · Clone and configure

```bash
git clone <your-repo-url> ml-feature-store
cd ml-feature-store
cp .env.example .env        # Edit .env if you want to change defaults
```

### 2 · Start all services

```bash
docker-compose up --build
```

Services start in order:  
`Zookeeper → Kafka → kafka-init (topic creation) → PostgreSQL → feature-client`

Wait for this log line from `feature_client`:
```
{"level":"INFO","msg":"Feature Store service is ready."}
```

### 3 · Verify

```bash
# Health check
curl http://localhost:8000/health

# OpenAPI docs
open http://localhost:8000/docs
```

---

## Running the Producer

The producer generates synthetic raw events and publishes them to the `raw-events` Kafka topic.

### Inside Docker (recommended)

```bash
# 200 events at 100 events/second (default)
docker-compose exec feature-client python producer.py

# Custom settings
docker-compose exec feature-client python producer.py \
  --count 1000 \
  --rate 200 \
  --entity-count 50
```

### Locally (host machine)

```bash
pip install confluent-kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer.py --count 500
```

### Producer CLI options

| Flag | Default | Description |
|---|---|---|
| `--count` | 200 | Total number of events to produce |
| `--rate` | 100.0 | Target events per second |
| `--entity-count` | 20 | Number of distinct entity_ids |

---

## API Reference

### `GET /health`

Returns the liveness status of the service and its components.

```bash
curl http://localhost:8000/health
```

**Response 200 (healthy)**
```json
{
  "status": "healthy",
  "db_connected": true,
  "kafka_consumer_running": true,
  "timestamp": "2024-06-01T09:00:00+00:00"
}
```

**Response 200 (degraded)** — returned when a component is down; service stays up but reports the problem.

---

### `GET /features/{entity_id}`

Retrieve all materialized ML features for the specified entity.

```bash
curl http://localhost:8000/features/user_0001
```

**Response 200**
```json
{
  "entity_id": "user_0001",
  "count": 5,
  "features": [
    {
      "entity_id": "user_0001",
      "feature_name": "last_action",
      "feature_value": "purchase",
      "timestamp": "2024-06-01T09:15:32.123456+00:00"
    },
    {
      "entity_id": "user_0001",
      "feature_name": "last_device",
      "feature_value": "mobile",
      "timestamp": "2024-06-01T09:15:32.123456+00:00"
    },
    {
      "entity_id": "user_0001",
      "feature_name": "last_event_timestamp",
      "feature_value": "2024-06-01T09:15:32+00:00",
      "timestamp": "2024-06-01T09:15:32.123456+00:00"
    },
    {
      "entity_id": "user_0001",
      "feature_name": "last_page_visited",
      "feature_value": "checkout",
      "timestamp": "2024-06-01T09:15:32.123456+00:00"
    },
    {
      "entity_id": "user_0001",
      "feature_name": "user_activity_count",
      "feature_value": "42",
      "timestamp": "2024-06-01T09:15:32.123456+00:00"
    }
  ]
}
```

**Response 404** — entity not yet seen
```json
{
  "detail": "No features found for entity_id='user_9999'. Ensure events have been produced and processed."
}
```

**Response 503** — database unavailable
```json
{
  "detail": "Feature retrieval failed due to a database error."
}
```

---

## Configuration

All configuration is provided via environment variables. Copy `.env.example` to `.env` and adjust:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address(es) |
| `RAW_EVENTS_TOPIC` | `raw-events` | Inbound raw events topic |
| `FEATURES_UPDATE_TOPIC` | `features-store-updates` | Outbound processed summaries topic |
| `KAFKA_CONSUMER_GROUP_ID` | `feature-store-consumer-group` | Consumer group ID |
| `KAFKA_CONSUMER_POLL_TIMEOUT` | `1.0` | Poll timeout in seconds |
| `POSTGRES_HOST` | `db` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `feature_store` | Database name |
| `POSTGRES_USER` | `user` | Database user |
| `POSTGRES_PASSWORD` | `password` | Database password |
| `POSTGRES_MAX_CONNECTIONS` | `10` | Max pooled connections |
| `POSTGRES_CONNECT_RETRY_ATTEMPTS` | `5` | Retry count on connection failure |
| `APP_HOST` | `0.0.0.0` | FastAPI bind host |
| `APP_PORT` | `8000` | FastAPI bind port |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG/INFO/WARNING/ERROR) |

---

## Design Decisions

### Kafka Topic Design

| Topic | Partitions | Purpose |
|---|---|---|
| `raw-events` | 3 | Inbound raw event stream (high throughput) |
| `features-store-updates` | 3 | Outbound feature-processing confirmations |

3 partitions allow future horizontal scaling to 3 consumer instances without reconfiguration.

### PostgreSQL Schema

```sql
features (
    entity_id     VARCHAR(255),   -- e.g. "user_0001"
    feature_name  VARCHAR(255),   -- e.g. "last_action"
    feature_value TEXT,           -- flexible: numbers, strings, timestamps
    timestamp     TIMESTAMPTZ,    -- when the feature was last computed
    PRIMARY KEY (entity_id, feature_name)
)
```

The composite primary key on `(entity_id, feature_name)` serves two purposes:
1. **Idempotency** — duplicate Kafka messages produce the same final state via `UPSERT`
2. **Performance** — O(1) point lookups for any specific feature of any entity

### Consumer Threading Model

The Kafka consumer runs as a **daemon thread** within the FastAPI process. This keeps deployment simple (single container) while maintaining modularity — the `FeatureConsumer` class has no coupling to FastAPI and can be extracted into a separate microservice by simply calling `consumer.start()` from a standalone `__main__` entry point.

### Feature Schema — Generic Key-Value

Using a generic `(entity_id, feature_name, feature_value TEXT)` schema instead of typed columns allows adding new feature types (e.g. `last_device`, `last_transaction_amount`) without schema migrations.

---

## Error Handling & Resilience

### Idempotency

All writes use `INSERT … ON CONFLICT (entity_id, feature_name) DO UPDATE SET …`. Re-processing the same Kafka message yields identical final state — no duplicates, no corrupted counts.

### Exponential Backoff with Jitter

Both the DB connection pool initialisation and the Kafka consumer poll error path use full-jitter exponential back-off:

```
sleep = random.uniform(0, min(cap, base * 2^attempt))
```

Jitter prevents thundering-herd issues when many consumer instances restart simultaneously.

### Message-Level Error Isolation

The consumer loop catches all exceptions at the per-message level:
- **JSON decode errors** → log warning, skip message, continue
- **Pydantic validation errors** → log warning, skip message, continue  
- **Database errors** → log error, skip message, continue (consumer stays alive)
- **Kafka poll errors** → log error, apply backoff, retry

The consumer thread only crashes on truly unrecoverable errors (e.g. out-of-memory), which trigger a container restart via Docker's `restart: on-failure` policy.

### Graceful Shutdown

On `SIGTERM` (e.g. `docker-compose down`):
1. FastAPI lifespan exit calls `consumer.stop()` → sets `threading.Event`
2. Consumer loop detects the event, calls `producer.flush(10s)` then `consumer.close()`
3. DB pool calls `closeall()`

---

## Running Tests

### Inside Docker (matches CI)

```bash
docker-compose exec feature-client pytest tests/ -v
```

### Locally (no Kafka/DB required — all mocked)

```bash
pip install -r requirements.txt
pytest tests/ -v
```

### Test coverage

```bash
pip install pytest-cov
pytest tests/ -v --cov=src --cov-report=term-missing
```

The test suite includes:

| Category | What's tested |
|---|---|
| Unit — `PostgreSQLManager` | connect with retry, save_feature SQL, batch UPSERT, get_features, error propagation |
| Unit — Feature extraction | count increment, metadata enrichment, missing metadata handling |
| Unit — `FeatureConsumer` | valid message → DB write, invalid JSON skip, Pydantic error skip, DB error survival |
| Integration — `/health` | healthy / degraded states |
| Integration — `/features/{id}` | 200 with records, 404 unknown entity, 503 DB down, 503 DB exception |
| Pydantic validation | field normalisation, blank field rejection, missing required fields |

---

## Performance

The system is designed to comfortably exceed **100 raw events/second**:

- **Confluent Kafka** (`librdkafka`) C bindings — minimal consumer overhead
- **Batch UPSERT** (`execute_values`) — single round-trip for all features per event
- **Connection pool** (`ThreadedConnectionPool`) — no connection setup latency per request
- **Composite PK index** — O(log n) writes, O(log n) reads

Benchmark to verify:

```bash
docker-compose exec feature-client python producer.py --count 2000 --rate 200
# Watch consumer logs for throughput
docker-compose logs -f feature_client
```

---

## API Screenshots

### Health Check — `GET /health`

```bash
$ curl -s http://localhost:8000/health | python -m json.tool
```

```json
{
    "status": "healthy",
    "db_connected": true,
    "kafka_consumer_running": true,
    "timestamp": "2024-06-01T09:00:00+00:00"
}
```

---

### Feature Retrieval — `GET /features/{entity_id}`

```bash
$ curl -s http://localhost:8000/features/user_0001 | python -m json.tool
```

```json
{
    "entity_id": "user_0001",
    "count": 5,
    "features": [
        {
            "entity_id": "user_0001",
            "feature_name": "last_action",
            "feature_value": "purchase",
            "timestamp": "2024-06-01T09:15:32.123456+00:00"
        },
        {
            "entity_id": "user_0001",
            "feature_name": "last_device",
            "feature_value": "mobile",
            "timestamp": "2024-06-01T09:15:32.123456+00:00"
        },
        {
            "entity_id": "user_0001",
            "feature_name": "last_event_timestamp",
            "feature_value": "2024-06-01T09:15:32+00:00",
            "timestamp": "2024-06-01T09:15:32.123456+00:00"
        },
        {
            "entity_id": "user_0001",
            "feature_name": "last_page_visited",
            "feature_value": "checkout",
            "timestamp": "2024-06-01T09:15:32.123456+00:00"
        },
        {
            "entity_id": "user_0001",
            "feature_name": "user_activity_count",
            "feature_value": "42",
            "timestamp": "2024-06-01T09:15:32.123456+00:00"
        }
    ]
}
```

---

### Entity Not Found — `GET /features/{entity_id}` (404)

```bash
$ curl -s http://localhost:8000/features/unknown_user | python -m json.tool
```

```json
{
    "detail": "No features found for entity_id='unknown_user'. Ensure events have been produced and processed."
}
```

---

### Swagger UI

Interactive API docs are auto-generated at: **http://localhost:8000/docs**

---

## Postman Collection

A ready-to-import Postman collection is included at [`postman_collection.json`](./postman_collection.json).

### Import & Run

1. Open Postman → **Import** → select `postman_collection.json`
2. Set the `base_url` variable to `http://localhost:8000`
3. Start all services: `docker-compose up --build`
4. Run the producer: `docker-compose exec feature-client python producer.py`
5. Click **Run Collection** in Postman

The collection includes automated test assertions for:
- HTTP status codes on all endpoints
- Response schema validation (all required fields present)
- `count` matches `features` array length
- 404 for unknown entities
