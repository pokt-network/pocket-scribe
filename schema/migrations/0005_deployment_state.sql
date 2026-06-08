-- +goose Up
-- +goose StatementBegin
-- ─────────────────────────────────────────────────────────────────────────────
-- Deployment metadata + indexer state.
--
-- Two tables:
--   - deployment        — IMMUTABLE config set once at bootstrap. One row.
--                         Identifies WHICH network this Postgres is indexing,
--                         what kind of genesis was used, and at what height.
--   - indexer_state     — MUTABLE live metrics. One row. Updated by reconciler.
--                         The "dashboard at a glance": chain head vs indexed head,
--                         lag, last query times, etc.
--
-- One view:
--   - gaps              — Heights missing from processed_heights per consumer
--                         (useful for debugging "why isn't consolidation advancing?").
--
-- Why two separate tables?
--   - `deployment` is write-once. Updating it should be a deliberate act
--     (re-bootstrap, network migration). It also drives the "wrong cluster"
--     safety check: consumers verify `chain_id + genesis_time` match config.
--   - `indexer_state` updates continuously (every minute). Separating it
--     keeps the immutable identity facts uncontaminated by churn.
--
-- See: ADR-020-deployment-metadata-and-indexer-state.md
-- ─────────────────────────────────────────────────────────────────────────────

-- ═══ deployment ════════════════════════════════════════════════════════════

CREATE TABLE deployment (
    -- Singleton: enforce one row.
    id                       SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),

    -- Network identity (matches configs/networks/<name>.yaml)
    network_id               TEXT NOT NULL,                       -- e.g. 'pocket-mainnet'
    chain_id                 TEXT NOT NULL,                       -- e.g. 'pocket' (verified at consumer startup)
    display_name             TEXT,                                -- e.g. 'Shannon Mainnet'

    -- Genesis configuration: how this deployment was bootstrapped.
    genesis_height           BIGINT NOT NULL,                     -- 1 for natural genesis; >1 for synthetic
    genesis_time             TIMESTAMPTZ NOT NULL,                -- chain time at genesis_height
    genesis_kind             TEXT NOT NULL
                             CHECK (genesis_kind IN (
                               'genesis_json',           -- parsed from genesis.json (start_height=1)
                               'synthetic_snapshot',     -- bulk gRPC snapshot at start_height>1
                               'archive_replay'          -- restored from FilePlugin cold archive
                             )),
    genesis_decoder_version  TEXT NOT NULL,                       -- e.g. 'v0_1_0' or 'v0_1_33'

    -- Bootstrap audit
    bootstrapped_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    bootstrapped_by_version  TEXT,                                -- which ps binary version ran the bootstrap
    bootstrapped_by_command  TEXT                                 -- e.g. 'ps migrate up && ps bootstrap-state --at-height=700000'
);

COMMENT ON TABLE deployment IS
$$Immutable per-deployment identity + bootstrap configuration. One row.

Consumers verify chain_id + genesis_time match their configured network at startup;
this protects against "wrong cluster" mistakes (e.g., pointing a mainnet-configured
ps consumer at a beta-configured Postgres).

To re-bootstrap (network switch, fresh full backfill): drop database, re-init.
There is NO graceful "change deployment in place".$$;

COMMENT ON COLUMN deployment.network_id IS
    'PocketScribe-internal logical name (e.g., pocket-mainnet, pocket-beta, pocket-localnet).';
COMMENT ON COLUMN deployment.chain_id IS
    'On-chain chain_id from /status .node_info.network. e.g. pocket, pocket-lego-testnet, poktroll.';
COMMENT ON COLUMN deployment.genesis_height IS
    'Height at which THIS deployment starts indexing. 1 = natural genesis; N = synthetic bootstrap at N.';
COMMENT ON COLUMN deployment.genesis_kind IS
    'genesis_json = parsed from chain genesis.json; synthetic_snapshot = bulk gRPC at >1; archive_replay = restored from FilePlugin cold archive.';
COMMENT ON COLUMN deployment.genesis_decoder_version IS
    'Decoder version that was current at genesis_height (e.g., v0_1_0 for mainnet genesis, v0_1_33 if synthetic-bootstrapped today).';

-- ═══ indexer_state ═════════════════════════════════════════════════════════

CREATE TABLE indexer_state (
    -- Singleton: enforce one row.
    id                          SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),

    -- Latest known chain state (updated by reconciler periodically).
    latest_chain_height         BIGINT,                             -- from chain /status
    latest_chain_time           TIMESTAMPTZ,
    latest_chain_queried_at     TIMESTAMPTZ,                        -- when we last polled chain
    latest_chain_query_error    TEXT,                               -- last error if chain unreachable

    -- Indexed progress (derived from processed_heights / consumer_consolidation).
    indexed_head_height         BIGINT,                             -- max(processed_heights.height) across all consumers
    safe_height                 BIGINT,                             -- min(consumer_consolidation.consolidated_up_to)
    indexed_head_updated_at     TIMESTAMPTZ,

    -- Lag (computed; cached for cheap dashboards).
    lag_blocks                  BIGINT,                             -- latest_chain_height - safe_height
    lag_seconds                 DOUBLE PRECISION,                   -- now() - block.time of safe_height

    -- Sealing health
    last_seal_at                TIMESTAMPTZ,                        -- max(bucket_seal.sealed_at)
    pending_dirty_buckets       BIGINT,                             -- count(cagg_dirty_buckets)

    -- Reconciler health
    last_reconciler_run_at      TIMESTAMPTZ,
    last_reconciler_drift_count INTEGER,                            -- 0 = clean; >0 = drift detected last run

    -- Self
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by                  TEXT                                -- which process updated last
);

COMMENT ON TABLE indexer_state IS
$$Live "dashboard at a glance" of indexing state. Updated by the reconciler /
sealing loop / a dedicated `ps state-updater` job every ~1 minute.

Source of truth for `ps doctor` summary, Hasura `indexer_state` query for ops
dashboards, and external monitoring (Prometheus scrape).

This is METADATA — not chain data. It's allowed to UPDATE (the rare exception
to the append-only rule), with `updated_at` and `updated_by` for audit.$$;

COMMENT ON COLUMN indexer_state.latest_chain_height IS
    'Latest known chain height from periodic /status poll. May lag a few seconds behind real-time.';
COMMENT ON COLUMN indexer_state.indexed_head_height IS
    'Highest height any consumer has processed (may be ahead of safe_height if some consumers lag).';
COMMENT ON COLUMN indexer_state.safe_height IS
    'Highest height ALL consumers have processed gap-free. Use for cross-entity-consistent queries.';
COMMENT ON COLUMN indexer_state.lag_blocks IS
    'latest_chain_height - safe_height. Cached for dashboard speed.';
COMMENT ON COLUMN indexer_state.lag_seconds IS
    'Wallclock seconds since the safe-height block was produced. The "how stale are we" number.';

-- Seed empty rows (one of each, immutable in count via id PK CHECK).
-- Real values are written by `ps bootstrap-state` and the state updater loop.
INSERT INTO indexer_state (id) VALUES (1) ON CONFLICT DO NOTHING;

-- ═══ gaps view ══════════════════════════════════════════════════════════════
-- For each consumer, expose contiguous "gap" regions in processed_heights.
-- This is the diagnostic for "why isn't consumer_consolidation advancing?"

CREATE OR REPLACE VIEW gaps AS
WITH grouped AS (
    SELECT
        consumer_name,
        height,
        height - ROW_NUMBER() OVER (PARTITION BY consumer_name ORDER BY height) AS grp
    FROM processed_heights
)
SELECT
    consumer_name,
    MIN(height) AS gap_after_height,                              -- last good height before the gap
    MAX(height) AS continuous_until_height,
    MAX(height) - MIN(height) + 1 AS continuous_block_count
FROM grouped
GROUP BY consumer_name, grp
ORDER BY consumer_name, gap_after_height;

COMMENT ON VIEW gaps IS
$$Per-consumer contiguous-range view of processed_heights. Useful for diagnosing
why `consumer_consolidation.consolidated_up_to` isn't advancing.

The "gap" is the space BETWEEN consecutive rows of this view per consumer:
if you see (1-100) then (105-200), heights 101-104 are missing.$$;

-- ═══ deployment_summary view ════════════════════════════════════════════════
-- Single denormalized "what is this deployment doing right now" row.

CREATE OR REPLACE VIEW deployment_summary AS
SELECT
    d.network_id,
    d.chain_id,
    d.display_name,
    d.genesis_height,
    d.genesis_kind,
    d.genesis_decoder_version,
    d.bootstrapped_at,
    s.latest_chain_height,
    s.latest_chain_time,
    s.indexed_head_height,
    s.safe_height,
    s.lag_blocks,
    s.lag_seconds,
    s.last_seal_at,
    s.pending_dirty_buckets,
    s.last_reconciler_run_at,
    s.last_reconciler_drift_count,
    s.updated_at AS state_updated_at
FROM deployment d, indexer_state s
WHERE d.id = 1 AND s.id = 1;

COMMENT ON VIEW deployment_summary IS
$$Denormalized one-row "deployment at a glance". What `ps doctor` shows,
what Hasura's default home query exposes, what external monitoring scrapes.$$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS deployment_summary;
DROP VIEW IF EXISTS gaps;
DROP TABLE IF EXISTS indexer_state;
DROP TABLE IF EXISTS deployment;
-- +goose StatementEnd
