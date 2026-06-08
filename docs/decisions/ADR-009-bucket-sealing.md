# ADR-009: Bucket sealing for continuous aggregates with `consumer_consolidation`

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

Continuous aggregates (hourly rewards, daily relays, etc.) are useful only when:
- Their bucket is **complete** (all underlying data present, no gaps).
- They reflect **late arrivals** correctly (if data arrives after sealing, re-aggregate).
- Refresh cost is **bounded** (one refresh per bucket per change, not per insert).

Timescale's built-in `add_continuous_aggregate_policy` refreshes on a schedule (e.g., every hour). For PocketScribe:
- Schedule-based refresh is **unaware of consumer gaps** — may refresh a bucket before consumer caught up → wrong values.
- Late arrivals slip through silently — bucket value remains stale.
- No audit trail of "when did this bucket become trustworthy?"

## Decision

Replace (or augment) Timescale's policy-driven refresh with a **gap-aware sealing loop**:
1. A `consumer_consolidation` table tracks the highest contiguous height per consumer.
2. A `bucket_seal` table records which aggregate buckets have been materialized and confirmed gap-free.
3. The **sealing loop** (a periodic Go process) iterates `aggregate_registry`, checks `consumer_consolidation` for each aggregate's `consumers_needed`, and refreshes + seals buckets only when all required consumers have caught up past the bucket's end.
4. Late arrivals enqueue invalidations in `cagg_dirty_buckets`; the sealing loop drains the queue and re-seals.

## Consequences

### Positive

- **No partial buckets exposed.** Queries that need consistency join with `bucket_seal` (or use the `safe_height` view).
- **Late arrivals handled deterministically.** Enqueue + re-seal = no silent staleness.
- **Bounded refresh cost.** One refresh per bucket per gap-close event, not per insert.
- **Audit trail.** `bucket_seal.sealed_at` + `sealed_by_consumers` tells you when each bucket became trustworthy.
- **Hierarchical aggregates work cleanly.** Daily buckets seal after hourly buckets seal; transitive via `depends_on`.
- **Decoupled from consumer rate.** Sealing runs every N minutes regardless of how fast consumers go.

### Negative

- **Additional Go process** (sealing loop) — singleton.
- **Additional tables** (`bucket_seal`, `cagg_dirty_buckets`) — small.
- **Slight delay** between "consumer caught up" and "bucket sealed" (one loop interval, e.g., 60s).
- **Requires consumers to write `processed_heights` rows** — small write overhead per block.

### Neutral

- The built-in Timescale policy can be **disabled** entirely (we control refresh ourselves) or kept with a long `start_offset` as a safety net.

## Alternatives considered

### Option A: Timescale built-in policy
- Pro: simpler.
- Con: gap-unaware; refreshes on a schedule.
- Con: late arrivals require manual `refresh_continuous_aggregate` calls.
- **Rejected because**: we need gap-awareness for trustworthy aggregates.

### Option B: Trigger-based invalidation only (no sealing loop)
- Pro: instant invalidation on insert.
- Con: triggers run inside the consumer's transaction → couples consumer to aggregate refresh logic.
- **Partially adopted**: triggers enqueue `cagg_dirty_buckets`; the loop processes the queue. Loop-driven, not trigger-driven.

### Option C: External job scheduler (cron, Airflow)
- Pro: visible scheduling.
- Con: external dependency; harder to keep in sync with `aggregate_registry`.
- **Rejected because**: in-binary loop is simpler and registry-driven.

## Implementation notes

### Tables (in `schema/migrations/0001_init.sql`):
- `aggregate_registry` — declarative catalog.
- `bucket_seal` — sealing record per (aggregate, bucket).
- `cagg_dirty_buckets` — invalidation queue.
- `consumer_consolidation` — per-consumer monotonic cursor.
- `processed_heights` — per-consumer per-height marker (feeds consolidation).

### Sealing loop algorithm

```python
loop every N seconds:
    drain dirty queue first  # late arrivals get priority
    
    for agg in registry.list(status='shadow' OR 'public'):
        for bucket in unsealed_buckets(agg):
            heights = heights_for(bucket.start, bucket.end)
            
            if heights.first is NULL:
                seal_empty(agg, bucket)   # chain halt = empty bucket = valid
                continue
            
            if all(c.consolidated_up_to >= heights.last for c in agg.consumers_needed):
                refresh_continuous_aggregate(agg.name, bucket.start, bucket.end)
                insert_or_update bucket_seal
```

### Consolidation maintenance

A background sub-loop computes `consumer_consolidation.consolidated_up_to` by scanning `processed_heights` for the highest contiguous height. Implementation: scan from current consolidated value forward, advance until a gap is found.

### Late arrivals

When a consumer inserts a row in an event hypertable:
```sql
INSERT INTO cagg_dirty_buckets (aggregate_name, bucket_start, bucket_end, reason)
SELECT name, time_bucket(bucket_size, $block_time),
       time_bucket(bucket_size, $block_time) + bucket_size,
       'late_arrival_height_' || $block_height
FROM aggregate_registry
WHERE $source_table = ANY(source_tables)
  AND time_bucket(bucket_size, $block_time) < (SELECT MAX(bucket_start_time) FROM bucket_seal WHERE aggregate_name = name)
ON CONFLICT DO NOTHING;
```

This insert happens **in the consumer's transaction** with the data insert, so atomicity is guaranteed.

## References

- Full session transcript: Topic 12, Topic 13.
- ADR-005 (append-only pure) — what enables late arrivals to be benign.
- ADR-006 (chain as source of truth) — what makes "complete bucket" meaningful.
- `docs/architecture/04-aggregates.md` — full design.
