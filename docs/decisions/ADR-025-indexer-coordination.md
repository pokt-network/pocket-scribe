# ADR-025: Indexer coordination — when is height H "fully indexed"?

**Status**: Proposed
**Date**: 2026-05-23
**Authors**: Jorge Cuesta, Claude

## Context

With fan-out publishing ([ADR-022](ADR-022-nats-payload-discipline.md)) and per-consumer batching ([ADR-024](ADR-024-consumer-batching.md)), each consumer advances its own `consolidated_up_to` cursor at its own pace. Different consumers cover different subject slices: the supplier consumer doesn't see bank events, the tokenomics consumer doesn't see session KV pairs, etc.

Several downstream concerns need a single answer to "is height H indexed?":

- **Bucket sealing** ([ADR-009](ADR-009-bucket-sealing.md)): a continuous aggregate for `[H_lo..H_hi]` can only seal once every relevant consumer has confirmed processing past `H_hi`.
- **Realtime push** (NATS WebSocket bridge): subscribers asking "tell me when H is committed" need a signal.
- **API freshness** (Hasura / PostgREST downstream): "what's the latest fully-indexed height?" must be answerable in O(1).
- **Reconciler** ([ADR-006](ADR-006-chain-as-source-of-truth.md)): drift checks pick a height to compare against. They need a height all consumers have finished.

Failure mode if coordination is missing: a consumer publishes a tokenomics aggregate referencing supplier state at H that the supplier consumer hasn't written yet. Foreign-key violation, or worse, a silent stale read if FK is deferred.

## Decision

PocketScribe defines a single global signal: **`indexed_height = MIN(consumer_consolidation.consolidated_up_to) FOR consumers WHERE participates_in_indexed_height = TRUE`**.

### Mechanism

1. **`consumer_consolidation`** is the single source of truth. Each consumer commits its row in the same transaction as its data rows (per [ADR-024](ADR-024-consumer-batching.md)).
2. A column `participates_in_indexed_height BOOLEAN NOT NULL DEFAULT TRUE` lets us register optional or shadow consumers without polluting the global signal (e.g. a new experimental consumer can run unmarked until validated).
3. A dedicated subprocess `ps indexed-height-publisher` polls `SELECT MIN(consolidated_up_to) FROM consumer_consolidation WHERE participates_in_indexed_height` every `min_publish_interval_ms` (default 200 ms). When the value advances, it:
   - Updates a singleton row in `indexer_state(global_indexed_height, updated_at)`.
   - Publishes `pokt.indexed.{H}` on NATS with `Nats-Msg-Id = "indexed-{H}"` (dedup-safe).
4. Downstream concerns subscribe / query as needed:
   - Bucket sealing reads `global_indexed_height` from `indexer_state`.
   - Realtime subscribers tail `pokt.indexed.>`.
   - APIs expose `global_indexed_height` as a view.

### Block envelope cross-check

The `pokt.block.{H}` envelope published by the sidecar carries `published_msg_count` (the total number of fan-out messages emitted for H across all subjects). Each consumer that subscribes to a subset can verify its own count is consistent with what it expects from the envelope (e.g. supplier consumer expects N supplier-related events out of total M). This is defense-in-depth: if a NATS message is silently dropped, the consumer detects a count mismatch BEFORE advancing the cursor, refusing the batch and forcing redelivery.

Implementation note: counts are computed in the sidecar from the parsed FilePlugin proto. They are not authoritative chain data — they are sidecar-emitted metadata for consumer self-check.

### What "participates" means

A consumer registers itself with the indexer at startup via:

```sql
INSERT INTO consumer_consolidation (consumer_name, consolidated_up_to, participates_in_indexed_height)
VALUES ($consumer_name, $start_height, TRUE)
ON CONFLICT (consumer_name) DO NOTHING;
```

`participates_in_indexed_height = FALSE` is set manually for:

- Experimental consumers under development.
- Shadow aggregates (per CLAUDE.md vocabulary) — materialized but not exposed.
- Backfill catch-up consumers running at a different lag than the live set.

The default is TRUE; the burden is on the operator to opt out, not in.

### Why MIN and not "everyone explicitly acks H"

Three reasons:

1. **No central commit phase**: a consumer doesn't broadcast "I'm done with H" — the cursor advancement IS that signal, and it's discoverable from one SELECT.
2. **Adding a new consumer doesn't require a coordinator restart**: it inserts its row, MIN naturally clamps to it until catch-up.
3. **Crash semantics are trivial**: a consumer that disappears mid-block freezes the MIN. Operator alerts, fixes, the cursor catches up, MIN unblocks.

## Consequences

### Positive

- O(1) query for "what's the latest fully-indexed height". One `SELECT global_indexed_height FROM indexer_state`.
- Downstream APIs and bucket sealing share the same signal; no risk of divergent definitions of "indexed".
- New consumers integrate by inserting a row; no schema migration, no coordinator change.
- Realtime push via NATS gives sub-second indexed-height updates without LISTEN/NOTIFY (preserving the project's ban on it for downstream APIs — internal use of `consumer_consolidation` polling is acceptable).

### Negative

- The MIN moves only as fast as the slowest consumer. A misbehaving consumer drags global progress. Mitigation: alerting on per-consumer lag (`consumer_consolidation.consolidated_up_to` vs `global_indexed_height`).
- 200 ms publisher poll adds a fixed floor to indexed-height latency. Acceptable for our use case; tunable.
- A consumer that subscribes to a subset must trust the sidecar's `published_msg_count`. The sidecar is a single producer per source, so trust is bounded.

## Open questions

- Whether to expose `pokt.indexed.{H}` as a stream or just a subject. Provisional: stream, with short retention (1 hour) so reconnecting subscribers can catch up briefly without spamming Postgres.
- Whether to publish individual per-consumer cursor advances too (`pokt.consumer.{name}.{H}`) for fine-grained observability. Provisional: no — Prometheus metrics cover this.

## References

- [ADR-005](ADR-005-append-only-pure.md) — append-only
- [ADR-007](ADR-007-per-module-consumers.md) — per-module consumers
- [ADR-009](ADR-009-bucket-sealing.md) — bucket sealing
- [ADR-022](ADR-022-nats-payload-discipline.md) — payload discipline
- [ADR-024](ADR-024-consumer-batching.md) — batching
- CLAUDE.md §5 (ack after commit), §"Consolidation" vocabulary entry
