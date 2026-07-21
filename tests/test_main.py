"""
tests/test_main.py
──────────────────
Comprehensive test suite covering:

  Unit Tests
  ──────────
  1. PostgreSQLManager — save_feature / save_features_batch / get_features
     (psycopg2 fully mocked — no real DB required)
  2. FeatureConsumer._extract_features — feature derivation logic
  3. FeatureConsumer._process_message  — valid / invalid message paths

  Integration Tests (in-process, via FastAPI TestClient)
  ───────────────────────────────────────────────────────
  4. GET /health  — db & consumer status reflected correctly
  5. GET /features/{entity_id} — 200, 404, 503 branches
  6. Unhandled exception handler

Run with:
  pytest tests/ -v
  # or inside Docker:
  docker-compose exec feature-client pytest tests/ -v
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from fastapi.testclient import TestClient

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures & helpers
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_ENTITY_ID = "user_0042"
SAMPLE_FEATURE_RECORDS = [
    {
        "entity_id":     SAMPLE_ENTITY_ID,
        "feature_name":  "last_action",
        "feature_value": "click",
        "timestamp":     datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
    },
    {
        "entity_id":     SAMPLE_ENTITY_ID,
        "feature_name":  "user_activity_count",
        "feature_value": "7",
        "timestamp":     datetime(2024, 1, 15, 12, 0, 5, tzinfo=timezone.utc),
    },
]


def _make_settings_mock():
    """Return a minimal mock of Settings."""
    s = MagicMock()
    s.postgres_dsn                       = "host=localhost dbname=test user=u password=p"
    s.postgres_min_connections           = 1
    s.postgres_max_connections           = 5
    s.postgres_connect_retry_attempts    = 2
    s.postgres_connect_retry_base_delay  = 0.01
    s.kafka_bootstrap_servers            = "kafka:29092"
    s.kafka_consumer_group_id            = "test-group"
    s.kafka_consumer_poll_timeout        = 0.1
    s.kafka_producer_delivery_timeout_ms = 5000
    s.raw_events_topic                   = "raw-events"
    s.features_update_topic              = "features-store-updates"
    return s


# ═════════════════════════════════════════════════════════════════════════════
# 1. Unit tests — PostgreSQLManager
# ═════════════════════════════════════════════════════════════════════════════

class TestPostgreSQLManager:
    """Tests for db_manager.PostgreSQLManager (psycopg2 fully mocked)."""

    def _make_manager(self):
        from src.db_manager import PostgreSQLManager
        settings = _make_settings_mock()
        return PostgreSQLManager(settings)

    # ── connect ───────────────────────────────────────────────────────────

    @patch("src.db_manager.psycopg2.pool.ThreadedConnectionPool")
    def test_connect_success(self, mock_pool_cls):
        """connect() creates the pool on first attempt."""
        mock_pool_cls.return_value = MagicMock()
        manager = self._make_manager()
        manager.connect()
        mock_pool_cls.assert_called_once()
        assert manager.is_connected

    @patch("src.db_manager.psycopg2.pool.ThreadedConnectionPool")
    @patch("src.db_manager.time.sleep", return_value=None)
    def test_connect_retries_on_failure(self, mock_sleep, mock_pool_cls):
        """connect() retries on OperationalError then succeeds."""
        import psycopg2
        mock_pool_cls.side_effect = [
            psycopg2.OperationalError("refused"),
            MagicMock(),   # succeeds on second attempt
        ]
        manager = self._make_manager()
        manager.connect()
        assert mock_pool_cls.call_count == 2
        assert manager.is_connected

    @patch("src.db_manager.psycopg2.pool.ThreadedConnectionPool")
    @patch("src.db_manager.time.sleep", return_value=None)
    def test_connect_raises_after_exhausting_retries(self, mock_sleep, mock_pool_cls):
        """connect() raises RuntimeError when all retry attempts fail."""
        import psycopg2
        mock_pool_cls.side_effect = psycopg2.OperationalError("refused")
        manager = self._make_manager()
        with pytest.raises(RuntimeError, match="Could not connect"):
            manager.connect()

    # ── save_feature ──────────────────────────────────────────────────────

    def test_save_feature_executes_upsert(self):
        """save_feature() executes the correct SQL with the given parameters."""
        manager = self._make_manager()

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cur  = MagicMock()

        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__  = Mock(return_value=False)
        manager._pool = mock_pool

        manager.save_feature(SAMPLE_ENTITY_ID, "last_action", "click")

        mock_cur.execute.assert_called_once()
        sql_arg = mock_cur.execute.call_args[0][0]
        assert "ON CONFLICT" in sql_arg
        assert "DO UPDATE" in sql_arg
        mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_save_feature_raises_on_db_error(self):
        """save_feature() re-raises psycopg2.Error to the caller."""
        import psycopg2
        manager = self._make_manager()

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.execute.side_effect = psycopg2.OperationalError("disk full")

        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__  = Mock(return_value=False)
        manager._pool = mock_pool

        with pytest.raises(psycopg2.Error):
            manager.save_feature(SAMPLE_ENTITY_ID, "x", "y")

    # ── save_features_batch ───────────────────────────────────────────────

    def test_save_features_batch_calls_execute_values(self):
        """save_features_batch() uses execute_values for efficiency."""
        manager = self._make_manager()

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__  = Mock(return_value=False)
        manager._pool = mock_pool

        with patch("src.db_manager.psycopg2.extras.execute_values") as mock_ev:
            manager.save_features_batch(
                SAMPLE_ENTITY_ID,
                {"last_action": "click", "user_activity_count": "3"},
            )
            mock_ev.assert_called_once()
            rows_arg = mock_ev.call_args[0][2]
            assert len(rows_arg) == 2

    # ── get_features ──────────────────────────────────────────────────────

    def test_get_features_returns_records(self):
        """get_features() returns a list of FeatureRecord objects."""
        manager = self._make_manager()

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchall.return_value = SAMPLE_FEATURE_RECORDS

        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__  = Mock(return_value=False)
        manager._pool = mock_pool

        records = manager.get_features(SAMPLE_ENTITY_ID)
        assert len(records) == 2
        assert records[0].feature_name in ("last_action", "user_activity_count")

    def test_get_features_returns_empty_list(self):
        """get_features() returns [] when no rows match the entity_id."""
        manager = self._make_manager()

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchall.return_value = []

        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__  = Mock(return_value=False)
        manager._pool = mock_pool

        records = manager.get_features("unknown_entity")
        assert records == []

    def test_is_connected_false_before_connect(self):
        manager = self._make_manager()
        assert not manager.is_connected


# ═════════════════════════════════════════════════════════════════════════════
# 2. Unit tests — Feature extraction logic
# ═════════════════════════════════════════════════════════════════════════════

class TestFeatureExtraction:
    """Tests for consumer._extract_features — no Kafka or DB involved."""

    def _extract(self, event_type: str, current_count: int = 0, metadata: dict = None):
        from src.consumer import _extract_features
        from src.models import RawEvent
        event = RawEvent(
            entity_id=SAMPLE_ENTITY_ID,
            event_type=event_type,
            timestamp=datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc),
            metadata=metadata or {},
        )
        return _extract_features(event, current_count)

    def test_basic_features_extracted(self):
        features = self._extract("click", current_count=4)
        assert features["user_activity_count"] == "5"
        assert features["last_action"] == "click"
        assert "last_event_timestamp" in features

    def test_activity_count_increments_from_zero(self):
        features = self._extract("login", current_count=0)
        assert features["user_activity_count"] == "1"

    def test_activity_count_increments_correctly(self):
        features = self._extract("purchase", current_count=99)
        assert features["user_activity_count"] == "100"

    def test_metadata_page_extracted(self):
        features = self._extract("view", metadata={"page": "checkout"})
        assert features.get("last_page_visited") == "checkout"

    def test_metadata_amount_extracted(self):
        features = self._extract("purchase", metadata={"amount": 49.99})
        assert features.get("last_transaction_amount") == "49.99"

    def test_metadata_device_extracted(self):
        features = self._extract("login", metadata={"device": "mobile"})
        assert features.get("last_device") == "mobile"

    def test_missing_metadata_keys_are_absent(self):
        features = self._extract("search", metadata={})
        assert "last_page_visited" not in features
        assert "last_transaction_amount" not in features


# ═════════════════════════════════════════════════════════════════════════════
# 3. Unit tests — FeatureConsumer._process_message
# ═════════════════════════════════════════════════════════════════════════════

class TestFeatureConsumerProcessMessage:
    """Tests for the message-processing path (Kafka + DB mocked)."""

    def _make_consumer(self):
        from src.consumer import FeatureConsumer
        settings = _make_settings_mock()
        db_mock = MagicMock()
        db_mock.get_features.return_value = []
        consumer = FeatureConsumer(settings, db_mock)
        consumer._producer = MagicMock()
        return consumer, db_mock

    def _make_msg(self, payload: dict | None, raw: bytes | None = None):
        msg = MagicMock()
        msg.error.return_value = None
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        if raw is not None:
            msg.value.return_value = raw
        else:
            msg.value.return_value = json.dumps(payload).encode()
        return msg

    def test_valid_message_stores_features(self):
        consumer, db_mock = self._make_consumer()
        payload = {
            "entity_id":  SAMPLE_ENTITY_ID,
            "event_type": "click",
            "timestamp":  "2024-06-01T09:00:00+00:00",
            "metadata":   {"page": "home"},
        }
        consumer._process_message(self._make_msg(payload))
        db_mock.save_features_batch.assert_called_once()
        args = db_mock.save_features_batch.call_args[0]
        assert args[0] == SAMPLE_ENTITY_ID
        assert "last_action" in args[1]
        assert "user_activity_count" in args[1]

    def test_invalid_json_is_skipped(self):
        consumer, db_mock = self._make_consumer()
        msg = self._make_msg(None, raw=b"not-json{{")
        consumer._process_message(msg)
        db_mock.save_features_batch.assert_not_called()

    def test_missing_required_field_skips_message(self):
        consumer, db_mock = self._make_consumer()
        # event_type is missing
        payload = {"entity_id": SAMPLE_ENTITY_ID, "timestamp": "2024-06-01T09:00:00+00:00"}
        consumer._process_message(self._make_msg(payload))
        db_mock.save_features_batch.assert_not_called()

    def test_db_error_does_not_crash_consumer(self):
        consumer, db_mock = self._make_consumer()
        db_mock.save_features_batch.side_effect = Exception("disk full")
        payload = {
            "entity_id":  SAMPLE_ENTITY_ID,
            "event_type": "purchase",
            "timestamp":  "2024-06-01T09:00:00+00:00",
        }
        # Should not raise
        consumer._process_message(self._make_msg(payload))

    def test_existing_count_is_incremented(self):
        from src.models import FeatureRecord
        consumer, db_mock = self._make_consumer()
        db_mock.get_features.return_value = [
            FeatureRecord(
                entity_id=SAMPLE_ENTITY_ID,
                feature_name="user_activity_count",
                feature_value="5",
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        ]
        payload = {
            "entity_id":  SAMPLE_ENTITY_ID,
            "event_type": "view",
            "timestamp":  "2024-06-01T09:00:00+00:00",
        }
        consumer._process_message(self._make_msg(payload))
        saved_features = db_mock.save_features_batch.call_args[0][1]
        assert saved_features["user_activity_count"] == "6"


# ═════════════════════════════════════════════════════════════════════════════
# 4 & 5. Integration tests — FastAPI endpoints (TestClient)
# ═════════════════════════════════════════════════════════════════════════════

class TestAPIEndpoints:
    """
    Integration tests using FastAPI TestClient.
    The PostgreSQLManager and FeatureConsumer are mocked so that no real
    Kafka or PostgreSQL is required.
    """

    @pytest.fixture(autouse=True)
    def _setup_app(self, monkeypatch):
        """
        Patch the module-level singletons in src.main before each test.
        """
        import src.main as main_module

        self.mock_db = MagicMock()
        self.mock_consumer = MagicMock()
        self.mock_consumer.is_running = True
        self.mock_db.is_connected = True

        monkeypatch.setattr(main_module, "_db_manager", self.mock_db)
        monkeypatch.setattr(main_module, "_feature_consumer", self.mock_consumer)

        # Bypass lifespan (startup/shutdown) for unit speed
        from src.main import app
        self.client = TestClient(app, raise_server_exceptions=False)

    # ── /health ───────────────────────────────────────────────────────────

    def test_health_returns_200_when_all_ok(self):
        response = self.client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "healthy"
        assert body["db_connected"] is True
        assert body["kafka_consumer_running"] is True

    def test_health_degraded_when_db_disconnected(self):
        self.mock_db.is_connected = False
        response = self.client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "degraded"
        assert body["db_connected"] is False

    def test_health_degraded_when_consumer_stopped(self):
        self.mock_consumer.is_running = False
        response = self.client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "degraded"
        assert body["kafka_consumer_running"] is False

    # ── /features/{entity_id} ────────────────────────────────────────────

    def test_get_features_returns_200_with_records(self):
        from src.models import FeatureRecord
        self.mock_db.get_features.return_value = [
            FeatureRecord(**r) for r in SAMPLE_FEATURE_RECORDS
        ]
        response = self.client.get(f"/features/{SAMPLE_ENTITY_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["entity_id"] == SAMPLE_ENTITY_ID
        assert body["count"] == 2
        feature_names = [f["feature_name"] for f in body["features"]]
        assert "last_action" in feature_names
        assert "user_activity_count" in feature_names

    def test_get_features_returns_404_for_unknown_entity(self):
        self.mock_db.get_features.return_value = []
        response = self.client.get("/features/nonexistent_user")
        assert response.status_code == 404
        assert "nonexistent_user" in response.json()["detail"]

    def test_get_features_returns_503_when_db_down(self):
        self.mock_db.is_connected = False
        response = self.client.get(f"/features/{SAMPLE_ENTITY_ID}")
        assert response.status_code == 503

    def test_get_features_returns_503_on_db_exception(self):
        self.mock_db.get_features.side_effect = Exception("connection reset")
        response = self.client.get(f"/features/{SAMPLE_ENTITY_ID}")
        assert response.status_code == 503

    def test_get_features_response_shape(self):
        """Verify the response matches the FeatureResponse schema."""
        from src.models import FeatureRecord
        self.mock_db.get_features.return_value = [
            FeatureRecord(**r) for r in SAMPLE_FEATURE_RECORDS
        ]
        response = self.client.get(f"/features/{SAMPLE_ENTITY_ID}")
        body = response.json()
        required_keys = {"entity_id", "features", "count"}
        assert required_keys.issubset(body.keys())
        for feature in body["features"]:
            assert "entity_id" in feature
            assert "feature_name" in feature
            assert "feature_value" in feature
            assert "timestamp" in feature

    # ── Edge cases ────────────────────────────────────────────────────────

    def test_entity_id_with_special_characters(self):
        self.mock_db.get_features.return_value = []
        # URL-encoded special chars should not crash the service
        response = self.client.get("/features/user%40example.com")
        assert response.status_code in (404, 422)

    def test_docs_endpoint_accessible(self):
        response = self.client.get("/docs")
        assert response.status_code == 200


# ═════════════════════════════════════════════════════════════════════════════
# 6. Pydantic model validation tests
# ═════════════════════════════════════════════════════════════════════════════

class TestRawEventValidation:
    """Tests that RawEvent rejects malformed input correctly."""

    def _make_valid(self, **overrides):
        from src.models import RawEvent
        data = {
            "entity_id":  "user_01",
            "event_type": "click",
            "timestamp":  datetime(2024, 1, 1, tzinfo=timezone.utc),
            "metadata":   {},
            **overrides,
        }
        return RawEvent(**data)

    def test_valid_event_parses_correctly(self):
        event = self._make_valid()
        assert event.entity_id == "user_01"
        assert event.event_type == "click"

    def test_entity_id_and_event_type_lowercased(self):
        event = self._make_valid(entity_id="USER_ABC", event_type="CLICK")
        assert event.entity_id == "user_abc"
        assert event.event_type == "click"

    def test_blank_entity_id_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._make_valid(entity_id="   ")

    def test_missing_timestamp_raises(self):
        from pydantic import ValidationError
        from src.models import RawEvent
        with pytest.raises(ValidationError):
            RawEvent(entity_id="u1", event_type="click")

    def test_metadata_defaults_to_empty_dict(self):
        from src.models import RawEvent
        event = RawEvent(
            entity_id="u1",
            event_type="view",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert event.metadata == {}
