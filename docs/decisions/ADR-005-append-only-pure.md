# ADR-005: Append-only pure state history (no `valid_to_height`)

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

Entity state (suppliers, applications, gateways, etc.) changes over time on-chain. We need to:
- Answer "what was the state of X at height N?" (point-in-time)
- Answer "what is X's current state?" (latest)
- Support out-of-order arrivals (late consumer, reconciler corrections, parallel backfill)
- Be safe under HA active-active producers (NATS may deliver duplicates within dedup window)
- Allow per-module reindex without affecting other modules

Two designs were debated:

**Option A (chosen)**: Append-only pure. Each chain change → new row. PK is `(address, block_height)`. No `valid_to_height` column. Validity ranges computed at query time with `LEAD()`.

**Option B**: Bi-temporal with materialized `valid_to_height`. Each new row UPDATEs the previous row's `valid_to_height`.

During the session, Claude initially conflated both designs. Jorge correctly caught the inconsistency: if you maintain `valid_to_height`, order of insertion matters. Insert 11 first → it sets `valid_to=NULL`. Insert 10 second → 10 has no awareness of 11 → both end up with `valid_to=NULL` (bug).

## Decision

Use **Option A: append-only pure**. No `valid_to_height` column on any `*_history` table.

Validity ranges, when needed, are derived at query time:

```sql
SELECT *,
  LEAD(block_height) OVER (PARTITION BY address ORDER BY block_height) - 1 AS valid_to_height
FROM supplier_history;
```

"Current state" via `DISTINCT ON`:

```sql
CREATE VIEW supplier AS
SELECT DISTINCT ON (address) *
FROM supplier_history
ORDER BY address, block_height DESC;
```

**Single exception**: `param_history` uses SCD2 with materialized `effective_to_height`. Justified because params change rarely (governance), and the materialized ranges are ergonomic for the dominant query pattern. Maintenance job is idempotent.

## Consequences

### Positive

- **Truly commutative.** Insert order does not affect final state. Insertions (10, 20, 30) and (30, 20, 10) produce identical tables.
- **Safe under late arrivals.** Consumer down for 3 hours → catches up → late inserts don't break sealed buckets (they invalidate them cleanly via `cagg_dirty_buckets`).
- **Safe under HA active-active.** Two archive nodes publishing the same data; NATS dedups; if dedup window misses, append-only + ON CONFLICT make it idempotent.
- **Reconciler corrections** simply insert authoritative snapshots without touching previous rows.
- **Per-module reindex** = `DELETE FROM <table> WHERE block_height BETWEEN X AND Y` then replay. No need to "fix" surrounding rows.
- **No UPDATE ever** on history tables → simpler reasoning, fewer locks, better Postgres performance under concurrency.

### Negative

- **`LEAD()` cost at query time** for range queries. Benchmarks show negligible at Pocket scale (~6k suppliers × N versions).
- **Slightly less ergonomic** for analytical queries that need explicit `valid_to_height`. Mitigated by providing a view (`supplier_history_with_ranges`) that adds the column at query time.
- **Storage**: same as bi-temporal (one row per change either way).

### Neutral

- The "what changed at this height" diff query is the same with or without `valid_to`.

## Alternatives considered

### Option B: Bi-temporal with `valid_to_height`
- Pro: Simpler queries (no `LEAD()`).
- **Con (fatal)**: Non-commutative. Out-of-order insertions break the invariant.
- **Rejected because**: out-of-order is the norm, not the exception, in this architecture (late arrivals, reconciler, HA).

### Option C: Bi-temporal with idempotent maintenance job
- Each insert just records `valid_from`. A separate job periodically computes `valid_to` for all rows via `LEAD()` and UPDATEs.
- Pro: Materialized `valid_to` for cheap range queries.
- Pro: Idempotent — converges regardless of insertion order.
- **Con**: Adds a maintenance process. Adds UPDATE pressure (acceptable at Pocket scale but real).
- **Con**: Lag between insert and `valid_to` materialization → cross-temporal queries can see stale ranges briefly.
- **Postponed**: if performance demands it, we can layer this on top of Option A. Adding denormalization later is easy; removing is hard.

## Implementation notes

- Schema invariant codified in `CLAUDE.md` (Hard Invariant 2).
- Schema designer agent (`pocketscribe-schema-designer`) rejects any `valid_to_*` column on history tables.
- Pre-commit invariant audit greps for the pattern and blocks commits.
- "Current state" views auto-generated via `DISTINCT ON` for every entity history table.
- A view `<entity>_history_with_ranges` is provided for analytics that need explicit ranges.

## References

- Full session transcript: Topic 10 (out-of-order), Topic 18 (correction of inconsistency).
- ADR-006 (chain as truth) is the partner principle.
- CLAUDE.md Hard Invariant 2.
