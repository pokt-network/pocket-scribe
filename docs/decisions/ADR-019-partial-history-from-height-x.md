# ADR-019: Optional partial-history indexing — start from height X

**Status**: Accepted (amended by [ADR-021](./ADR-021-shannon-history-discontinuity.md) — `synthetic_snapshot` is the recommended default on mainnet because genesis sync is not reachable)
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

By default PocketScribe indexes from genesis (height 1) onwards. But operators have legitimate reasons to start from a later height:

- **Quick catch-up**: a fresh deployment that needs to be useful within hours, not the 6-12 hours of a full genesis-to-tip sync.
- **Disk / cost constraints**: Mainnet currently has 700k+ blocks; in years it'll be millions. An operator who only needs the last 90 days saves significant storage.
- **Use-case scope**: an analytics deployment focused on "current ecosystem health" doesn't need 2024-era data.
- **Spike / experimentation**: a developer evaluating PocketScribe doesn't want to wait for full sync.

## Decision

PocketScribe supports an **optional `start_height` per network config**. When set:

1. Bootstrap queries the chain for the **full state of every indexed entity at `start_height`** (bulk `ListSuppliers --height=X`, `ListApplications --height=X`, etc. via gRPC).
2. Each entity's first row in `*_history` carries `block_height = start_height` and `snapshot_method = 'start_height_bootstrap'`.
3. Consumers skip every chain message where `block_height < start_height`.
4. Reconciler only operates on heights ≥ start_height.
5. Queries for state/events at heights < start_height return an explicit "out of indexing window" sentinel (or simply NULL, depending on the API).
6. The `upgrades` table still tracks all upgrades for documentation purposes, but the router's effective range starts at start_height.

Default (`start_height` unset or = 1) preserves current behavior: full history from genesis.

## Consequences

### Positive

- **Fast onboarding**: hours instead of days for full backfill.
- **Bounded storage**: pay only for the history you need.
- **Multiple deployments per network**: one "full history" deployment for archival, one "last 30 days" for hot analytics — same chain, same code, different `start_height`.
- **Trivially backwards-compatible**: existing genesis-from-1 deployments unaffected.

### Negative

- **Bootstrap requires bulk gRPC queries** at `start_height`. For modules with thousands of entities (6k+ suppliers on mainnet), bootstrap may take minutes.
- **Pagination required**: gRPC `List<Entity>` often paginates; bootstrap must handle this.
- **Cannot answer "what was state at height Y < start_height"** — query returns out-of-window. Documented in API responses.
- **Aggregates have a hard lower bound** at `start_height`. Buckets that would span the boundary are partial (or excluded).
- **Reconciler false-positives**: if reconciler queries chain at a height where bootstrap is in-progress, mismatches may appear briefly. Mitigation: bootstrap acquires an exclusive lock; reconciler waits.
- **Cannot reduce start_height after deployment without full re-bootstrap**. Increasing it is easy (drop earlier rows + advance cursor).

### Neutral

- The `block` table contains rows only from `start_height` onwards.
- Tilt dev environments default to `start_height: 1` (localnet always starts at genesis).

## Alternatives considered

### Option A: Always start from genesis; no partial-history mode
- Pro: simpler model.
- Con: forces hours-long bootstrap for every deployment.
- Con: doesn't match real operational needs.
- **Rejected**: operators have legitimate use cases for partial history.

### Option B: Support "rolling window" (auto-drop rows older than N days)
- Pro: bounded storage forever.
- Con: complicates reconciler (window edge mutates).
- Con: aggregates become moving targets.
- **Deferred**: may add later as `retention_height_offset`, but `start_height` is the simpler first feature.

### Option C: Allow snapshot import (e.g., from another PocketScribe deployment)
- Pro: skip bulk gRPC at bootstrap.
- Con: yet another protocol; integrity validation needed.
- **Deferred**: `pg_dump` between deployments works for now.

## Implementation notes

### Config

```yaml
# configs/networks/mainnet.yaml
network:
  id: pocket-mainnet
  chain_id: pocket
  genesis_height: 1
  genesis_decoder_version: v0_1_0
  # start_height: 700000           # optional; defaults to genesis_height (1)
```

### Bootstrap subcommand

```bash
ps bootstrap-state \
  --config configs/networks/mainnet.yaml \
  --at-height 700000

# Internally:
#   1. Query block at height 700000 for time
#   2. For each indexed module:
#      a. List<Entity> --height=700000 (paginated)
#      b. INSERT into <entity>_history with snapshot_method='start_height_bootstrap'
#   3. Mark all consumers as consolidated_up_to = 700000 - 1
#   4. Initialize block table with the boundary row
#   5. Done; consumers can now start from 700000 onwards
```

This subcommand is **idempotent**: running it twice produces the same DB state (uses `ON CONFLICT DO UPDATE`).

### Consumer behavior

```go
func (c *Consumer) processMessage(msg Message) error {
    if msg.BlockHeight < c.startHeight {
        // Below the indexing window. Ack and skip.
        return msg.Ack()
    }
    // ... normal processing
}
```

NATS-side optimization: the sidecar can publish to subjects with the height in the subject name (`pokt.kv.supplier.700123`), and consumers can subscribe with a filter that ignores low heights. But sidecar runs ahead of all consumers, so it doesn't know any one consumer's start_height — easier to filter at the consumer.

### Reconciler

```go
func (r *Reconciler) ListIndexed(ctx context.Context, h int64) (map[string]*Snapshot, error) {
    if h < r.startHeight {
        return nil, ErrBelowIndexingWindow
    }
    // ... normal flow
}
```

### Query semantics

API responses for "supplier X at height Y" where `Y < start_height`:

```json
{
  "error": "OUT_OF_INDEXING_WINDOW",
  "start_height": 700000,
  "requested_height": 500000,
  "message": "This deployment indexes from height 700000 onward. For earlier data, query a deployment with a lower start_height."
}
```

Or in Hasura/PostgREST: queries return empty result sets (no row matches `block_height <= Y` when `Y < start_height`).

### Aggregates

For `rewards_hourly`, the first bucket containing `start_height` is **partial** (it doesn't include events from before start_height). Two options:

- **A**: Exclude that bucket from aggregates (skip sealing it).
- **B**: Include it but mark `is_partial = true` in `bucket_seal`.

Default: **A** (cleaner). Operator can override per-aggregate.

## References

- User request: "Podriamos ser capaces de decir 'indexa apartir del height X'?"
- ADR-006 (chain as source of truth) — bootstrap snapshots come from chain.
- ADR-009 (bucket sealing) — sealing respects start_height.
- ADR-018 (no hardcoded upgrades) — partial-history is compatible because upgrades table is DB-driven.
- **ADR-021** (Shannon history discontinuity) — makes snapshot-bootstrap the mandatory mainnet path; the `start_height` mechanism in this ADR is the implementation that supports it. `recommended_start_height: 102142` documented in `configs/networks/mainnet.yaml`.
- `configs/networks/*.yaml` — `start_height` optional field.
