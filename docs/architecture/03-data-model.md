# 03 — Data Model

## Core principle

> **The chain is the source of truth. The indexer is a mirror. Every row records what the chain said at a specific height, with the time the chain said it.**

This principle generates all the schema rules below.

## Universal columns (every table)

| Column | Type | Purpose |
|---|---|---|
| `block_height` | `BIGINT NOT NULL` | Chain-derived cursor axis. Used for joins, gaps, cursor advance. |
| `block_time` | `TIMESTAMPTZ NOT NULL` | Chain consensus time (from Tendermint header). Used for `time_bucket()`. |
| `indexed_at` | `TIMESTAMPTZ DEFAULT now()` | **Audit only**. Never queried, grouped, or filtered in business logic. |

**Rule**: `block_time` comes from the chain header, **never** from indexer write time. The exception `indexed_at` exists for debugging only.

## Three table categories

### Category A — Coordination & meta tables

These track the indexer's own state. Mostly small, mostly mutable (cursors).

- `block` — `(height, time, hash, proposer_address, tx_count)`. Bridges height ↔ time.
- `processed_heights` — `(consumer_name, height, processed_at)`. Per-consumer cursor.
- `consumer_consolidation` — `(consumer_name, consolidated_up_to)`. Highest contiguous height without gaps. Maintained by a background loop.
- `aggregate_registry` — `(name, source_tables, depends_on, bucket_size, consumers_needed, sql_template, status, backfill_state)`. Declarative aggregate catalog.
- `bucket_seal` — `(aggregate_name, bucket_start_time, ..., sealed_at, sealed_by_consumers)`. Materialization status per bucket.
- `cagg_dirty_buckets` — `(aggregate_name, bucket_start, bucket_end, dirty_since, reason)`. Invalidation queue.
- `param_history` — `(module, name, value, effective_from_height, effective_to_height)`. Governance-driven module params (SCD2 because params change rarely).
- `upgrades` — `(name, applied_at_height, applied_at_time, decoder_version)`. Chain upgrade ledger; powers the version router.

### Category B — Entity history (append-only)

These represent entities with a lifecycle on-chain: Supplier, Application, Gateway, Service, Session, Validator, Account, etc.

**Schema invariants**:
- PK: `(address_or_id, block_height)`
- Append-only: never `UPDATE`. Each chain change → new row.
- No `valid_to_height`. Validity computed at query time with `LEAD()`.
- Snapshot is the **whole** entity state at that height (not a diff).
- Nullable fields for forward compatibility (new versions add fields).
- Carries `triggered_by_*` columns for auditability.

Example:

```sql
CREATE TABLE supplier_history (
    address                    TEXT NOT NULL,
    block_height               BIGINT NOT NULL,
    block_time                 TIMESTAMPTZ NOT NULL,
    -- chain-emitted snapshot
    owner_address              TEXT NOT NULL,
    operator_address           TEXT,
    stake_upokt                NUMERIC(78, 0) NOT NULL,
    services                   JSONB NOT NULL,        -- array of SupplierServiceConfig
    rev_share                  JSONB,
    unstake_session_end_height BIGINT,
    services_activation_heights JSONB,
    -- procedence / audit
    triggered_by_event         TEXT,                   -- 'EventSupplierStaked', etc.
    triggered_by_tx_hash       TEXT,
    snapshot_method            TEXT NOT NULL,          -- 'streaming_service' | 'rpc_query' | 'reconciler_correction' | 'genesis'
    proto_version              TEXT NOT NULL,          -- 'v0.0.10', 'v0.1.0', ...
    indexed_at                 TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (address, block_height)
);
CREATE INDEX ON supplier_history (address, block_height DESC);
CREATE INDEX ON supplier_history (block_height);
```

### "Current state" view (per entity type)

```sql
CREATE VIEW supplier AS
SELECT DISTINCT ON (address) *
FROM supplier_history
ORDER BY address, block_height DESC;
```

This is the canonical "current". Hasura and PostgREST expose this view, not `supplier_history` directly (history is a separate `supplier_history` view if needed for analytics).

### "State at height Y" query

```sql
SELECT DISTINCT ON (address) *
FROM supplier_history
WHERE block_height <= $Y
ORDER BY address, block_height DESC;
```

For one specific entity:

```sql
SELECT * FROM supplier_history
WHERE address = $1 AND block_height <= $2
ORDER BY block_height DESC LIMIT 1;
```

### Optional derived view: validity ranges

When some downstream consumer really needs explicit `valid_to_height`:

```sql
CREATE VIEW supplier_history_with_ranges AS
SELECT *,
  LEAD(block_height) OVER (
    PARTITION BY address ORDER BY block_height
  ) - 1 AS valid_to_height
FROM supplier_history;
```

This is **derived at query time**, never materialized as a column.

### Category C — Event hypertables (time-series)

Events that are facts (immutable records of something that happened on-chain): `EventClaimSettled`, `EventProofSubmitted`, `MintBurnOp`, `EventSupplierSlashed`, etc.

**Schema invariants**:
- Hypertable partitioned by `block_time`.
- PK is composite, deterministic: `(block_height, tx_index, event_index)` or equivalent.
- Append-only by nature (events don't update).
- Compressed older chunks (>30 days) with TimescaleDB columnar compression.
- Nullable fields for forward compatibility.

Example:

```sql
CREATE TABLE event_claim_settled (
    block_height          BIGINT NOT NULL,
    block_time            TIMESTAMPTZ NOT NULL,
    tx_index              INTEGER NOT NULL,
    event_index           INTEGER NOT NULL,
    -- chain-emitted fields
    session_id            TEXT NOT NULL,
    supplier_address      TEXT NOT NULL,
    application_address   TEXT NOT NULL,
    service_id            TEXT NOT NULL,
    settled_upokt         NUMERIC(78, 0) NOT NULL,
    -- added in later proto versions (nullable for backwards compat)
    mint_ratio            NUMERIC,                -- NULL for heights < v0.0.12
    num_estimated_relays  BIGINT,                 -- NULL for heights < v0.1.0
    minted_upokt          NUMERIC(78, 0),         -- NULL for heights < v0.1.0
    overservicing_loss    NUMERIC(78, 0),         -- NULL for heights < v0.1.5
    deflation_loss        NUMERIC(78, 0),         -- NULL for heights < v0.1.5
    -- audit
    proto_version         TEXT NOT NULL,
    indexed_at            TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (block_height, tx_index, event_index)
);

SELECT create_hypertable('event_claim_settled', 'block_time');

ALTER TABLE event_claim_settled SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'supplier_address',
    timescaledb.compress_orderby   = 'block_height'
);
-- compression policy enabled AFTER backfill stabilizes:
-- SELECT add_compression_policy('event_claim_settled', INTERVAL '30 days');

CREATE INDEX ON event_claim_settled (supplier_address, block_time DESC);
CREATE INDEX ON event_claim_settled (application_address, block_time DESC);
```

## Why no `valid_to_height` in history tables

**Commutativity** is the property that lets the indexer survive every operational scenario we care about:
- Late arrivals (consumer was down)
- Reconciler corrections
- Reindex partials (range deletes + replay)
- Multi-source publishing (HA active-active producers)
- Backfill running parallel to live

If we materialized `valid_to_height`, every insert at height N would need to UPDATE the previous row's `valid_to`. That UPDATE depends on knowing which previous row → depends on order of arrival → breaks under all the above scenarios.

The append-only pure model (no `valid_to`) is **truly commutative**: inserts can arrive in any order, the final table is identical. The `LEAD()` query reconstructs ranges deterministically.

The cost is paying for `LEAD()` at query time. Benchmarks show this is negligible at Pocket's scale (~6k suppliers × N versions). If a specific analytical query suffers, we materialize **that specific** view, not the base table.

## Why the snapshot is full state, not a delta

If we stored deltas (`stake_delta = +5000`), reconstructing state at height N requires summing all deltas up to N. This is what Pocketdex does today, and it's the source of:
- Math bugs causing permanent drift
- Inability to validate "state at height N" against the chain
- Reindex requirement for any bug fix

Storing full snapshots means:
- "State at height N" is one row lookup, exact.
- A bug in the snapshot extraction only affects that snapshot's row, not all subsequent ones.
- Reconciliation = compare row vs chain query, exact equality.

## Param history (SCD2)

Module params change rarely (governance proposals) but aren't entity lifecycle either. They use Slowly Changing Dimension type 2:

```sql
CREATE TABLE param_history (
    module                TEXT NOT NULL,
    name                  TEXT NOT NULL,
    value                 JSONB NOT NULL,
    effective_from_height BIGINT NOT NULL,
    effective_from_time   TIMESTAMPTZ NOT NULL,
    effective_to_height   BIGINT,            -- NULL = currently effective
    effective_to_time     TIMESTAMPTZ,
    triggered_by_tx_hash  TEXT,
    proto_version         TEXT NOT NULL,
    PRIMARY KEY (module, name, effective_from_height)
);
```

**This is the one exception** to "no `valid_to` column" — params are not on a hot path, change infrequently, and benefit from materialized ranges for ergonomics. The maintenance job that closes ranges is idempotent.

Query "value at height N":

```sql
SELECT value FROM param_history
WHERE module = $1 AND name = $2
  AND effective_from_height <= $3
  AND (effective_to_height IS NULL OR effective_to_height > $3);
```

## Genesis state

Genesis is a special height (effectively `block_height = 0`). The genesis.json is parsed once at indexer startup (or via a one-time migration script) to populate initial state:

- Accounts, balances → `account_history`, `balance_history` at height 0
- Initial validators → `validator_history` at height 0
- Pre-existing suppliers/apps/gateways (if any) → respective `*_history` at height 0
- Module params → `param_history` with `effective_from_height = 0`

Each row gets `snapshot_method = 'genesis'`.

## Forbidden patterns

- ❌ `UPDATE supplier_history SET valid_to_height = X` — never. Append, don't mutate.
- ❌ `ALTER TABLE supplier_history DROP COLUMN x` — never. Make nullable and stop populating.
- ❌ `time_bucket('1h', now())` or `time_bucket('1h', indexed_at)` — must be `block_time`.
- ❌ `INSERT ... SELECT SUM(...)` to "recompute" totals — never. Snapshots come from the chain.
- ❌ Caching "current" entity state in Redis — the `DISTINCT ON` view is the source of truth; if Redis is needed for performance, ensure cache invalidation, but the DB always wins.

## Migration discipline

- All migrations are forward-only, numbered (`0001_init.sql`, `0002_add_supplier_revshare.sql`).
- No `DROP TABLE`, no `DROP COLUMN`.
- Adding a column is `ADD COLUMN ... NULL`.
- Tested in CI by running every migration in order against a fresh DB.
- Documented in `docs/decisions/` if non-trivial.
