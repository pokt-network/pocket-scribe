# ADR-006: Chain as source of truth (snapshots, not event-derived state)

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

Pocketdex (and many indexers) derive entity state from events:

```go
case EventSupplierStakeIncreased:
    UPDATE supplier SET stake = stake + event.amount WHERE address = event.address
```

This pattern is **the source of every major bug in 6 years of Pocket indexing work**:
- Off-by-one in the math → permanent drift.
- Missed event type → entity state diverges from chain.
- Order-of-operations bug → wrong intermediate state visible until next event fires.
- Can't validate "is my indexer correct?" without comparing every entity to the chain.

The chain itself computes state and writes it to its KV store. The ABCI StreamingService (ADR-003) gives us access to those KV writes.

## Decision

The indexer **never computes derived state**. For any entity (Supplier, Application, etc.), the indexer snapshots the **full entity state from the chain** at the height where it changed.

Sources of authoritative snapshots:
1. **ABCI StreamingService KV writes** (preferred): the chain emits the new value of each affected key per block.
2. **gRPC bulk queries** (for backfill and reconciliation): `ListSuppliers(height=H)` returns full state for all suppliers at H.

Events are **triggers** (telling us "this entity changed at this height"), not data.

## Consequences

### Positive

- **Impossible to drift.** Each snapshot is independently verifiable against the chain.
- **Math bugs become local.** A bug in snapshot extraction affects only that row; doesn't propagate to subsequent rows (because each is independent).
- **Per-row reindex** = re-snapshot from chain at that height. Trivial.
- **Reconciler can validate.** "Did supplier X's stake at height H match the chain at the same height?" is a single comparison.
- **Property-based testing trivial.** "For 100 random (entity, height) pairs, indexed value == chain query." If this fails, find and fix.
- **Survives chain protocol changes.** New TLM math added in v0.2.0? No problem — we always snapshot, we don't compute.

### Negative

- **Requires archive node** for gRPC backfill (no pruning). Cost: disk space (200-400GB for Shannon mainnet today).
- **Bulk query is a fallback only** — for hot path, we rely on StreamingService which doesn't need RPC. Reconciler uses RPC every ~10 min, low load.
- **Storage cost** of full snapshots vs. deltas: ~3-5x more space per change. Mitigated by TimescaleDB columnar compression (10x typical).

### Neutral

- "Current state" views still expose the same shape as Pocketdex did, so downstream API consumers don't notice.

## Alternatives considered

### Option A: Event-derived mutations (Pocketdex approach)
- Pro: Less storage.
- **Con (fatal)**: Drift is permanent and silent.
- **Rejected because**: this is the root cause of every major past incident.

### Option B: Bulk `ListSuppliers` per block (legacy approach)
- Pro: Bulk query, no per-entity RPC storm.
- Con: Transfers full state every block even if 2 entities changed → wasteful at 6k+ entities.
- Con: Doesn't expose per-key diffs cleanly.
- **Rejected for hot path**; **kept for reconciliation** (every 10 min, not every block).

### Option C: Per-entity gRPC on event (Patrón C as initially proposed)
- Pro: Authoritative per entity.
- **Con (fatal)**: 3k supplier restake in one block = 3k RPC calls per block. Indexer dies.
- **Rejected because**: doesn't scale to bulk operator actions.

### Option D: Event-derived + periodic full snapshot reconciliation
- Pro: Cheaper storage; reconciler catches drift.
- Con: Drift exists between reconciliation passes — wrong values visible to clients.
- Con: Reconciliation overwrite is jarring (entities suddenly change value).
- **Rejected because**: client-visible inconsistency is unacceptable.

## Implementation notes

- ABCI StreamingService → NATS → consumer decodes `StoreKVPair` → upserts full snapshot.
- For each entity type, the decoder reads the KV value (full proto) and produces a canonical `*Snapshot` type.
- Snapshot includes `triggered_by_event` and `triggered_by_tx_hash` columns for auditability — we know which event prompted this snapshot, but the **value** comes from the KV write.
- Reconciler runs `poktrolld query supplier list-supplier --height H` periodically, compares with `supplier` view, alerts on diff, optionally re-snapshots.

## References

- Full session transcript: Topic 6 (RPC storm problem), Topic 7 (StreamingService verification).
- ADR-005 (append-only pure) is the schema-level partner.
- ADR-009 (bucket sealing) builds on the gap-detection enabled by per-consumer cursors.
- CLAUDE.md Hard Invariant 3.
