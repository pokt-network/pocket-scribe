# 04 — Aggregates & Bucket Sealing

## The pattern in one paragraph

Aggregates are **declarative** (registered in `aggregate_registry`), **time-bucketed** (always on `block_time`), and **gap-aware** (sealed only when every required consumer has confirmed processing past the bucket's last height). Late arrivals invalidate sealed buckets via a queue; the sealing loop re-refreshes them. Rolling windows (24h/48h/72h) are composed at query time from sealed buckets + live tail, not separate aggregates.

## Why not just use Timescale's built-in policy?

Timescale's `add_continuous_aggregate_policy` refreshes on a schedule (e.g. every hour). For PocketScribe:

- Schedule-based refresh is **unaware of gaps**. It may refresh a bucket before all data for that bucket has been processed → wrong values until next refresh.
- Late arrivals slip through silently.
- No audit trail of "when did this bucket become trustworthy?"

PocketScribe's sealing loop replaces (or augments) the built-in policy with **gap-aware, gated refreshes**. The native policy can stay enabled with a long `start_offset` as a backup safety net, but the sealing loop is the primary driver.

## The aggregate registry

```sql
CREATE TABLE aggregate_registry (
    name              TEXT PRIMARY KEY,
    description       TEXT,
    source_tables     TEXT[] NOT NULL,
    depends_on        TEXT[] DEFAULT ARRAY[]::TEXT[],
    bucket_size       INTERVAL NOT NULL,
    consumers_needed  TEXT[] NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('shadow','public','deprecated')),
    sealed_strategy   TEXT NOT NULL CHECK (sealed_strategy IN ('eager','lazy','manual')),
    backfill_state    JSONB,
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now()
);
```

**Fields**:
- `source_tables` — which hypertables / tables this aggregate reads from. Used by the late-arrival invalidator.
- `depends_on` — other aggregates this depends on (for hierarchical aggregates).
- `consumers_needed` — which consumers must be consolidated past the bucket's `height_range_last` before sealing.
- `status` — `shadow` (computed, not exposed), `public` (exposed via API), `deprecated` (legacy, scheduled for removal).
- `sealed_strategy` — `eager` (seal as soon as possible, every loop pass), `lazy` (seal only when there's at least one late arrival or first computation), `manual` (only when `refresh_continuous_aggregate` is called by hand).

## Materialized view template

```sql
CREATE MATERIALIZED VIEW <name>
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('<bucket_size>', block_time) AS bucket_start,
    <dimension_columns>,
    <aggregation_columns>,
    -- height bounds MANDATORY for gap detection
    MIN(block_height) AS first_height_in_bucket,
    MAX(block_height) AS last_height_in_bucket,
    COUNT(*) AS row_count
FROM <source_table>
GROUP BY bucket_start, <dimension_columns>;

-- Enable real-time aggregation so queries see live tail without explicit refresh
ALTER MATERIALIZED VIEW <name>
SET (timescaledb.materialized_only = false);
```

**Invariants**:
- `time_bucket(..., block_time)` — **never** `now()`, `clock_timestamp()`, `indexed_at`.
- Include `MIN/MAX(block_height)` so the sealing loop can check completeness.
- Include `COUNT(*)` for observability (empty buckets vs no-data buckets).

## The sealing loop

Runs every ~60 seconds (configurable). For each aggregate in `status IN ('shadow', 'public')`:

```python
for agg in registry.list_active():
    for bucket in unsealed_or_dirty_buckets(agg):
        h_first, h_last = heights_for_time_range(bucket.start, bucket.end)
        
        if h_first is None:
            # No blocks in this bucket = chain halt for that period
            seal_empty(agg, bucket)
            continue
        
        all_consolidated = all(
            consolidation_for(c).consolidated_up_to >= h_last
            for c in agg.consumers_needed
        )
        
        if not all_consolidated:
            continue  # try next loop
        
        # Refresh just this bucket (deterministic, idempotent)
        db.exec(f"CALL refresh_continuous_aggregate('{agg.name}', %s, %s)",
                bucket.start, bucket.end)
        
        # Insert/update the seal
        db.exec("""
            INSERT INTO bucket_seal (aggregate_name, bucket_start_time, bucket_end_time,
                                     height_range_first, height_range_last,
                                     sealed_at, sealed_by_consumers)
            VALUES (%s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (aggregate_name, bucket_start_time) DO UPDATE SET
                sealed_at = EXCLUDED.sealed_at,
                height_range_first = EXCLUDED.height_range_first,
                height_range_last = EXCLUDED.height_range_last,
                sealed_by_consumers = EXCLUDED.sealed_by_consumers
        """, agg.name, bucket.start, bucket.end, h_first, h_last,
             agg.consumers_needed)
        
        # If this bucket was in the dirty queue, dequeue it
        db.exec("DELETE FROM cagg_dirty_buckets WHERE aggregate_name=%s AND bucket_start=%s",
                agg.name, bucket.start)
```

## Late arrival handling

When a consumer inserts a row into a hypertable with a `block_time` that falls in an already-sealed bucket:

1. Consumer detects the late insert (or a periodic scan detects it).
2. Enqueues invalidation:
   ```sql
   INSERT INTO cagg_dirty_buckets (aggregate_name, bucket_start, bucket_end, reason)
   SELECT registry.name,
          time_bucket(registry.bucket_size, $block_time),
          time_bucket(registry.bucket_size, $block_time) + registry.bucket_size,
          'late_arrival_height_' || $block_height
   FROM aggregate_registry registry
   WHERE registry.status IN ('shadow', 'public')
     AND $source_table = ANY(registry.source_tables)
   ON CONFLICT (aggregate_name, bucket_start) DO NOTHING;
   ```
3. Sealing loop drains `cagg_dirty_buckets` on its next pass → re-refreshes the bucket.

This is debounced naturally: if 20 late arrivals all fall in the same bucket within one loop interval, they enqueue once (ON CONFLICT) and trigger one refresh.

## Hierarchical aggregates

Higher buckets derive from lower buckets, **not** from raw data:

```sql
-- 1h aggregate over raw events
CREATE MATERIALIZED VIEW rewards_hourly WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', block_time) AS bucket_start,
    supplier_address,
    SUM(settled_upokt) AS total_settled,
    MIN(block_height) AS first_height_in_bucket,
    MAX(block_height) AS last_height_in_bucket,
    COUNT(*) AS row_count
FROM event_claim_settled
GROUP BY bucket_start, supplier_address;

-- 1d aggregate over 1h aggregate (NOT raw)
CREATE MATERIALIZED VIEW rewards_daily WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket_start) AS bucket_start,
    supplier_address,
    SUM(total_settled) AS total_settled,
    MIN(first_height_in_bucket) AS first_height_in_bucket,
    MAX(last_height_in_bucket) AS last_height_in_bucket
FROM rewards_hourly
GROUP BY bucket_start, supplier_address;
```

In the registry:

```sql
INSERT INTO aggregate_registry VALUES
  ('rewards_hourly', ..., ARRAY['event_claim_settled'], ARRAY[]::TEXT[], '1 hour'::interval, ARRAY['tokenomics'], 'public', ...),
  ('rewards_daily',  ..., ARRAY['rewards_hourly'],     ARRAY['rewards_hourly'], '1 day'::interval, ARRAY['tokenomics'], 'public', ...);
```

The sealing loop processes lower aggregates first (`depends_on` is a topological hint).

**Benefits**:
- Daily refresh is fast (24 rows in hourly → 1 row in daily per dimension).
- Late arrivals propagate: invalidate hourly → next refresh updates hourly → invalidate daily for the affected day → next pass refreshes daily.

## Rolling windows (24h, 48h, 72h relative to "now")

**Do NOT create a separate aggregate**. Compose at query time:

```sql
-- "rewards last 24h for supplier X"
SELECT supplier_address, SUM(total_settled) AS total
FROM rewards_hourly
WHERE bucket_start > now() - INTERVAL '24 hours'
  AND supplier_address = $1
GROUP BY supplier_address;
```

With `timescaledb.materialized_only = false`, this query automatically unions:
- Materialized buckets (older than the current incomplete bucket).
- Live raw data for the current incomplete bucket (no manual refresh needed).

For "rolling 30 days for the supplier leaderboard":

```sql
SELECT supplier_address, SUM(total_settled) AS total
FROM rewards_daily
WHERE bucket_start > now() - INTERVAL '30 days'
GROUP BY supplier_address
ORDER BY total DESC
LIMIT 100;
```

## Queries that must not see partial buckets

Use the `bucket_seal` table or the `safe_height` view:

```sql
-- "complete weekly totals only"
SELECT r.*
FROM rewards_weekly r
JOIN bucket_seal s
  ON s.aggregate_name = 'rewards_weekly'
  AND s.bucket_start_time = r.bucket_start;
```

Or mark uncertainty:

```sql
SELECT r.*, (s.sealed_at IS NOT NULL) AS is_sealed
FROM rewards_weekly r
LEFT JOIN bucket_seal s
  ON s.aggregate_name = 'rewards_weekly'
  AND s.bucket_start_time = r.bucket_start;
```

The client decides what to do with `is_sealed = false`.

## Shadow → public lifecycle

New aggregates start in `status = 'shadow'`:

1. Migration creates the materialized view and registers with `status='shadow'`.
2. Sealing loop begins materializing it.
3. Operator validates: pick 5 random sealed buckets, manually compute expected values from raw data, compare with aggregate output. Must match exactly.
4. After validation passes (typically 1 week of running):
   ```sql
   UPDATE aggregate_registry SET status = 'public', updated_at = now() WHERE name = 'X';
   ```
5. Hasura / PostgREST exposes views filtered by `status = 'public'`.

## Active aggregates (registered)

| Name | Bucket | Source | Consumers | Status |
|---|---|---|---|---|
| `rewards_hourly` | 1h | `event_claim_settled` | tokenomics | shadow (initial) |
| `rewards_daily` | 1d | `rewards_hourly` | tokenomics | shadow |
| `claims_hourly` | 1h | `event_claim_settled` | tokenomics | shadow |
| `proofs_hourly` | 1h | `event_proof_updated` | tokenomics | shadow |
| `mints_burns_daily` | 1d | `mint_burn_op` | tokenomics | shadow |
| `relays_supplier_daily` | 1d | `event_claim_settled` | tokenomics | shadow |

(Add to this table when adding new aggregates via `/scaffold-aggregate`.)

## Backfill for new aggregates

When a new aggregate is registered after the indexer has been running:

1. Migration creates the empty materialized view.
2. The sealing loop will only seal **newly populated** buckets, not historical ones.
3. To backfill historical buckets:
   ```sql
   -- Manually refresh in batches (avoid one huge refresh that locks for hours)
   DO $$
   DECLARE
     t TIMESTAMPTZ := '2024-01-01';
     end_t TIMESTAMPTZ := now() - INTERVAL '1 day';
     step INTERVAL := '7 days';
   BEGIN
     WHILE t < end_t LOOP
       CALL refresh_continuous_aggregate('rewards_hourly', t, t + step);
       t := t + step;
     END LOOP;
   END $$;
   ```
4. Then mark all backfilled buckets as sealed (bulk insert into `bucket_seal` based on `consumer_consolidation`).

This is wrapped in a script: `scripts/aggregate-backfill.sh <name> <from> <to>`.

## Compression policy (apply AFTER backfill)

```sql
-- Apply compression to chunks older than 30 days
SELECT add_compression_policy('event_claim_settled', INTERVAL '30 days');

-- For aggregates, compress historical materialized rows
ALTER MATERIALIZED VIEW rewards_hourly SET (timescaledb.compress = true);
SELECT add_compression_policy('rewards_hourly', INTERVAL '30 days');
```

**Important**: don't enable compression during backfill. Inserting into compressed chunks is slow (decompress + recompress). Enable as a post-stabilization step.

## Operational notes

- **Sealing loop is a singleton.** Don't run multiple `ps sealing` instances against the same DB. If you must scale, partition the aggregate_registry by name range.
- **Sealing loop checkpoints to** `bucket_seal` itself — it's stateless and idempotent. Crash recovery: just restart.
- **Monitor**: `pocketscribe_sealing_buckets_sealed_total`, `pocketscribe_sealing_loop_duration_seconds`, `pocketscribe_cagg_dirty_buckets_pending`.
