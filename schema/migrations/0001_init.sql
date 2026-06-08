-- +goose Up
-- +goose StatementBegin
-- ─────────────────────────────────────────────────────────────────────────────
-- PocketScribe — Foundation schema
-- ─────────────────────────────────────────────────────────────────────────────
-- This migration establishes the coordination + metadata tables that all other
-- migrations depend on. Entity history tables and event hypertables are NO
-- LONGER defined here — they are produced by the /generate-migration-from-diff
-- skill from versioned proto snapshots (ADR-028).
--
-- Invariants encoded here:
--   1. Every entity / event row carries (block_height, block_time).
--   2. Append-only with PK including block_height (no valid_to_*).
--   3. Idempotent inserts via ON CONFLICT (pk).
--   4. block_time always comes from chain consensus header.
--
-- See: docs/architecture/03-data-model.md, docs/decisions/ADR-028-*.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- The Rosetta stone between block_height and block_time.
CREATE TABLE IF NOT EXISTS block (
    height            BIGINT PRIMARY KEY,
    time              TIMESTAMPTZ NOT NULL,
    hash              TEXT NOT NULL UNIQUE,
    proposer_address  TEXT,
    tx_count          INTEGER NOT NULL DEFAULT 0,
    indexed_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS block_time_idx ON block (time);
COMMENT ON TABLE block IS 'Authoritative mapping from block_height to block_time (chain consensus header time).';
COMMENT ON COLUMN block.time IS 'Consensus time from Tendermint header. Never indexer write time.';

-- Per-consumer per-height cursor. One row per processed height per consumer.
CREATE TABLE IF NOT EXISTS processed_heights (
    consumer_name  TEXT NOT NULL,
    height         BIGINT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, height)
);
CREATE INDEX IF NOT EXISTS processed_heights_height_idx ON processed_heights (consumer_name, height DESC);
COMMENT ON TABLE processed_heights IS 'Per-consumer per-height marker. Insert in same tx as data. Used for gap detection.';

-- Per-consumer monotonic cursor — highest contiguous height with no gaps.
CREATE TABLE IF NOT EXISTS consumer_consolidation (
    consumer_name        TEXT PRIMARY KEY,
    consolidated_up_to   BIGINT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE consumer_consolidation IS 'Highest height per consumer where all preceding heights are processed. Used by sealing.';

-- Aggregate registry — declarative catalog of all continuous aggregates.
CREATE TABLE IF NOT EXISTS aggregate_registry (
    name                TEXT PRIMARY KEY,
    description         TEXT,
    source_tables       TEXT[] NOT NULL,
    depends_on          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    bucket_size         INTERVAL NOT NULL,
    consumers_needed    TEXT[] NOT NULL,
    status              TEXT NOT NULL DEFAULT 'shadow'
                        CHECK (status IN ('shadow', 'public', 'deprecated')),
    sealed_strategy     TEXT NOT NULL DEFAULT 'lazy'
                        CHECK (sealed_strategy IN ('eager', 'lazy', 'manual')),
    backfill_state      JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE aggregate_registry IS 'Declarative catalog of continuous aggregates. Sealing loop iterates this.';

-- Bucket seal — records that a specific aggregate bucket has been materialized.
CREATE TABLE IF NOT EXISTS bucket_seal (
    aggregate_name       TEXT NOT NULL,
    bucket_start_time    TIMESTAMPTZ NOT NULL,
    bucket_end_time      TIMESTAMPTZ NOT NULL,
    height_range_first   BIGINT,
    height_range_last    BIGINT,
    sealed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed_by_consumers  TEXT[] NOT NULL,
    PRIMARY KEY (aggregate_name, bucket_start_time),
    FOREIGN KEY (aggregate_name) REFERENCES aggregate_registry(name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS bucket_seal_sealed_at_idx ON bucket_seal (sealed_at DESC);
COMMENT ON TABLE bucket_seal IS 'Marks a continuous aggregate bucket as materialized and gap-free.';

-- Invalidation queue for late arrivals that touch a sealed bucket.
CREATE TABLE IF NOT EXISTS cagg_dirty_buckets (
    aggregate_name   TEXT NOT NULL,
    bucket_start     TIMESTAMPTZ NOT NULL,
    bucket_end       TIMESTAMPTZ NOT NULL,
    dirty_since      TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason           TEXT,
    PRIMARY KEY (aggregate_name, bucket_start),
    FOREIGN KEY (aggregate_name) REFERENCES aggregate_registry(name) ON DELETE CASCADE
);
COMMENT ON TABLE cagg_dirty_buckets IS 'Invalidation queue. Sealing loop drains this to re-refresh affected buckets.';

-- Module params history (SCD2) — governance-driven parameter changes.
-- ONE documented exception to "no valid_to_*" rule, kept for ergonomics.
CREATE TABLE IF NOT EXISTS param_history (
    module                  TEXT NOT NULL,
    name                    TEXT NOT NULL,
    value                   JSONB NOT NULL,
    effective_from_height   BIGINT NOT NULL,
    effective_from_time     TIMESTAMPTZ NOT NULL,
    effective_to_height     BIGINT,
    effective_to_time       TIMESTAMPTZ,
    triggered_by_tx_hash    TEXT,
    proto_version           TEXT NOT NULL,
    indexed_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (module, name, effective_from_height)
);
CREATE INDEX IF NOT EXISTS param_history_module_name_idx ON param_history (module, name, effective_from_height DESC);
COMMENT ON TABLE param_history IS 'Module params (SCD2). Documented exception to no-valid_to rule for ergonomics.';

-- Upgrades table — chain upgrade ledger; powers the version router.
-- NOTE: prior 0004 migration removes the legacy hardcoded INSERTs; this table
-- is populated at runtime by `ps sync-upgrades` (ADR-018).
CREATE TABLE IF NOT EXISTS upgrades (
    name                TEXT PRIMARY KEY,
    applied_at_height   BIGINT NOT NULL UNIQUE,
    applied_at_time     TIMESTAMPTZ NOT NULL,
    decoder_version     TEXT NOT NULL,
    notes               TEXT,
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS upgrades_height_idx ON upgrades (applied_at_height);
COMMENT ON TABLE upgrades IS 'Chain upgrade ledger. internal/router uses this to dispatch to the correct decoder.';

-- A safe height view — min consolidated across all consumers.
CREATE OR REPLACE VIEW safe_height AS
SELECT MIN(consolidated_up_to) AS height
FROM consumer_consolidation;
COMMENT ON VIEW safe_height IS 'Min consolidated height across all consumers. Cross-entity-consistent queries should filter by block_height <= (SELECT height FROM safe_height).';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP VIEW IF EXISTS safe_height;
DROP TABLE IF EXISTS upgrades;
DROP TABLE IF EXISTS param_history;
DROP TABLE IF EXISTS cagg_dirty_buckets;
DROP TABLE IF EXISTS bucket_seal;
DROP TABLE IF EXISTS aggregate_registry;
DROP TABLE IF EXISTS consumer_consolidation;
DROP TABLE IF EXISTS processed_heights;
DROP TABLE IF EXISTS block;

-- +goose StatementEnd
