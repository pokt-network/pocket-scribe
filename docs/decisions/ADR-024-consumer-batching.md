# ADR-024: Consumer batching discipline

**Status**: Accepted (Slice 1 Phase E, 2026-06-09)
**Date**: 2026-05-23
**Authors**: Jorge Cuesta, Claude

## Context

[ADR-022](ADR-022-nats-payload-discipline.md) commits to fan-out publishing: one NATS message per event / KV / tx. A heavy mainnet block can produce hundreds of messages on a single consumer's subscription, and the chain is expected to grow ~10× in throughput while ALSO redistributing claim/proof load uniformly across blocks (current bunching will be smoothed by ongoing protocol work).

Naive consumer = one Postgres transaction per NATS message would:

- Inflate transaction count proportional to event volume.
- Saturate Postgres WAL with tiny commits.
- Multiply network round-trips between consumer and DB.
- Break the "ack after commit" invariant if we tried to amortize by acking out of order.

The chain naturally provides a coalescing boundary: a height H. Every consumer subscribing to fan-out subjects for H receives a finite, knowable set of messages, ending logically when the `pokt.block.{H}` envelope (or the sidecar's per-height "fence" signal) arrives.

## Decision

Each consumer maintains an in-memory batch buffer keyed by `(subject_subset, block_height)` and flushes via one of three triggers:

1. **Block boundary (primary)**: when the `pokt.block.{H}` envelope arrives, the consumer flushes all buffered messages for `H` in a single Postgres transaction, advances its cursor in `consumer_consolidation`, and acks all the buffered NATS messages.
2. **Size cap (safety)**: if buffered rows exceed `batch_max_rows` (default 5000), flush partial — write rows, but DO NOT advance the cursor; keep messages unacked until the block envelope closes the height.
3. **Time cap (liveness)**: if the oldest buffered message exceeds `batch_max_age_ms` (default 5000 ms) without a block envelope, flush partial under the same rules as the size cap. Indicates a sidecar stall — emit metric `pocketscribe_batch_partial_flush_total{reason="time"}`.

### Write pattern

Use `pgx.CopyFrom` for bulk insert. For idempotency, rely on the unique constraint from the deterministic primary key (`(block_height, tx_index, event_index)` or per-entity `(address, block_height)`). On conflict, `DO NOTHING`. Cosmos-style upsert with `DO UPDATE SET` only where the schema requires it (cursor table, registry).

### Ack discipline

```
1. BEGIN tx
2. CopyFrom rows
3. UPDATE consumer_consolidation SET consolidated_up_to = H WHERE consumer = $1
4. COMMIT
5. (loop) Ack each buffered NATS message
6. Clear in-memory buffer for H
```

If step 5 fails partway, the buffer is reconstructed from NATS redelivery (idempotency carries us through). If step 4 fails before commit, the buffer is dropped — NATS will redeliver. If step 4 succeeds but the process crashes before step 5, NATS redelivers, the consumer batches again, but the COPY hits the unique constraint and is a no-op, the cursor update is `WHERE consolidated_up_to < H` so it's a no-op, then the consumer acks normally. All paths are idempotent.

### Cursor table

`consumer_consolidation` has one row per `(consumer_name, instance_id)`:

```sql
CREATE TABLE consumer_consolidation (
    consumer_name      TEXT        NOT NULL,
    consolidated_up_to BIGINT      NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name)
);
```

`consolidated_up_to` advances monotonically. The "indexed height" of a consumer is exactly this value.

## Consequences

### Positive

- One Postgres transaction per (consumer, block) regardless of message count. WAL pressure scales with blocks not events.
- Block-boundary flushes give natural alignment with the bucket-sealing pattern in [ADR-009](ADR-009-bucket-sealing.md): an aggregate sealing for `[H_lo, H_hi]` knows that every consumer past `H_hi` is fully done with the buffered messages.
- Per-consumer pace: a slow consumer can lag without blocking faster ones; cursors advance independently.

### Negative

- Memory pressure proportional to the largest block × number of subscribed subjects. At ~5000 rows the buffer is ~5 MB per consumer — bounded. A 10× scaling would push toward 50 MB; still acceptable.
- A stalled sidecar (no block envelope arriving) means partial flushes WITHOUT cursor advance — consumer rows accumulate in Postgres but the cursor doesn't progress. Operator sees this via metric + bucket sealing not progressing. Recovery: investigate sidecar; once the envelope arrives, cursor catches up.

## Open questions

- How to expose batch metrics: per-consumer histograms (flush size, flush latency, partial-flush count).
- Whether to make `batch_max_rows` / `batch_max_age_ms` per-consumer config or global.
- Whether the block envelope on `pokt.block.{H}` should carry a `published_msg_count` field so consumers can sanity-check they received everything before the flush (cross-check with what they expected). Provisional answer: yes — see [ADR-025](ADR-025-indexer-coordination.md).

## References

- [ADR-005](ADR-005-append-only-pure.md) — append-only state
- [ADR-007](ADR-007-per-module-consumers.md) — per-module consumers
- [ADR-009](ADR-009-bucket-sealing.md) — bucket sealing
- [ADR-022](ADR-022-nats-payload-discipline.md) — payload discipline
- [ADR-025](ADR-025-indexer-coordination.md) — indexer coordination
- CLAUDE.md §5 — "ack after commit" invariant

## Amendment (Phase E, 2026-06-09): implementation scoping

Phase E implements the block-boundary fence (trigger 1) in
`internal/consumer/batch.go`. The size cap (trigger 2) and time cap (trigger 3)
partial-flush valves are deferred to Phase G hardening — bootstrap replays are
bounded and the envelope follows the fan-out immediately. The buffer dedups
redeliveries by Nats-Msg-Id so an AckWait redelivery cannot double-buffer.
Quiet heights (zero fan-out messages for a consumer's filters) flush an EMPTY
batch when the envelope arrives — this is what advances the supplier cursor
over heights with no supplier activity.
