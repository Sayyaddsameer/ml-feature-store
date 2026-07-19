-- =============================================================
-- Feature Store Schema
-- =============================================================
-- The composite PRIMARY KEY on (entity_id, feature_name)
-- guarantees idempotent upserts and O(1) point lookups.
-- The idx_entity_id index accelerates "give me ALL features
-- for entity X" range scans.
-- =============================================================

CREATE TABLE IF NOT EXISTS features (
    entity_id     VARCHAR(255) NOT NULL,
    feature_name  VARCHAR(255) NOT NULL,
    feature_value TEXT         NOT NULL,
    timestamp     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_id, feature_name)
);

CREATE INDEX IF NOT EXISTS idx_entity_id ON features (entity_id);
