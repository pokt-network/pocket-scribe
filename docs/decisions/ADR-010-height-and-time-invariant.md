# ADR-010: Every row carries `(block_height, block_time)` from chain consensus

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta (rule from 6 years of indexer experience), Claude

## Context

Every analytical query an indexer serves is fundamentally time-based or height-based. The data has two natural axes:
- **`block_height`** — deterministic chain cursor; used for joins, gaps, exact state at height.
- **`block_time`** — consensus time from the Tendermint header; used for `time_bucket()`, time-range queries, dashboards.

A third axis is dangerous: **indexer write time** (`now()`, `clock_timestamp()`, `indexed_at`). Using it as a queryable axis means:
- Reproducibility breaks (re-running indexer produces different rows at the time axis).
- Backfill data has very different write times than live data, but the same chain semantics.
- Time-range queries answer different things depending on when the indexer caught up.
- Drift between indexers replicating the same chain.

Jorge's experience (6 years building Cosmos indexers): **all real-world analytics resolve "what blocks fell in time X-Y" → bound queries by their height range**. Using write time as the axis caused silent correctness bugs.

## Decision

**Every row in every PocketScribe table carries both `block_height` and `block_time`, both NOT NULL, both sourced from the chain.**

- `block_height` = `BIGINT NOT NULL`. From `RequestFinalizeBlock.Height`.
- `block_time` = `TIMESTAMPTZ NOT NULL`. From `RequestFinalizeBlock.Time` (Tendermint header time).

Indexer write time may exist as `indexed_at TIMESTAMPTZ DEFAULT now()` — **audit metadata only**. Never appears in:
- `WHERE` clauses on hot query paths.
- `GROUP BY` clauses.
- `time_bucket()` calls.
- Continuous aggregate definitions.
- Index keys.

The `block` table acts as the Rosetta stone: `SELECT MIN(height), MAX(height) FROM block WHERE time BETWEEN ...`.

## Consequences

### Positive

- **Reproducibility.** Indexer can be torn down and replayed from genesis; every row ends up with identical `block_time` and `block_height` values.
- **Determinism across replicas.** Two PocketScribe instances indexing the same chain produce identical row content.
- **Time-range queries are predictable.** "Rewards last 24h" returns the same answer regardless of when the indexer started or whether it's caught up.
- **Backfill is invisible.** A row written during backfill (months after the block was created) has the same time/height as a row written live.
- **Cross-axis queries are trivial.** "All events for height range [H1, H2]" and "All events for time range [T1, T2]" use the same data.

### Negative

- **One extra column** (`block_time`) on every table. Negligible storage.
- **Indexer must always know `block_time`** when inserting. Solved by passing it through the processing pipeline from the block header.

### Neutral

- `block_height` is already on every row in most indexers; adding `block_time` makes it explicit.

## Alternatives considered

### Option A: Only `block_height`, derive `block_time` via JOIN with `block` table
- Pro: One less column.
- Con: Every analytical query needs a join. Slow on aggregate tables.
- Con: Continuous aggregates can't `time_bucket()` without joining.
- **Rejected because**: time is a hot query axis; denormalizing is correct.

### Option B: Only `block_time`, derive `block_height` via JOIN with `block` table
- Same join cost issue.
- Plus: `block_time` is not unique (chain halts → same time for many blocks → can't recover height).
- **Rejected because**: height is the determinism axis; can't lose it.

### Option C: Use `indexed_at` as the time axis
- Pro: No need to plumb chain time through code.
- **Con (fatal)**: Breaks reproducibility; backfill rows have very different `indexed_at` than live rows for the same block.
- **Rejected because**: this is the source of bugs Jorge experienced over 6 years.

## Implementation notes

- All schema migrations include `block_height BIGINT NOT NULL` and `block_time TIMESTAMPTZ NOT NULL` on every new entity / event table.
- The pre-commit invariant audit greps for `time_bucket(` patterns and flags any that don't use `block_time`.
- The schema designer agent (`pocketscribe-schema-designer`) refuses to design tables that don't include both.
- Documented as Hard Invariant 1 in CLAUDE.md.

## The canonical query pattern

```sql
-- "rewards between time T1 and T2"
WITH bounds AS (
    SELECT MIN(height) AS h_first, MAX(height) AS h_last
    FROM block
    WHERE time >= $1 AND time < $2
)
SELECT supplier_address, SUM(settled_upokt)
FROM event_claim_settled, bounds
WHERE block_height BETWEEN bounds.h_first AND bounds.h_last
GROUP BY supplier_address;
```

The first CTE converts time → height (cheap, indexed). The second uses the height bounds (BIGINT BETWEEN PK = optimal). Same query gives identical results regardless of when the indexer processed the rows.

## References

- Full session transcript: Topic 15 (rule articulated by Jorge, saved as memory).
- CLAUDE.md Hard Invariant 1.
- Memory file: `~/.claude/projects/-home-overlordyorch/memory/feedback_blockchain_indexer_time_axis.md`.
