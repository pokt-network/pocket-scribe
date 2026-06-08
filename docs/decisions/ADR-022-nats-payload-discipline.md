# ADR-022: NATS payload discipline (per-event fan-out, no whole-block bodies)

**Status**: Proposed
**Date**: 2026-05-23
**Authors**: Jorge Cuesta, Claude

## Context

The sidecar (`ps fileplugin`) reads per-block FilePlugin output (`block-{H}-meta` + `block-{H}-data`) and must publish to NATS. Naive options:

- **A — one msg per file**: publish the entire `block-{H}-meta` proto as one NATS message on `pokt.block.{H}`. Same for `block-{H}-data`. Consumers download the blob and parse client-side.
- **B — fan-out per event/KV**: parse the proto in the sidecar and publish one NATS message per logical unit (event, tx, StoreKVPair). Subjects encode the type so consumers subscribe to slices of interest.

NATS limits:

- Default `max_payload = 1 MiB`. Hard cap 64 MiB but treated as anti-pattern by the project for indexer workloads.
- Mainnet today emits blocks with hundreds of events; one block's meta file already routinely exceeds 256 KiB and will grow ~10× as relay/proof volume scales (and additionally redistributes as settlement is spread across blocks, per ongoing protocol work).

Option A puts unbounded payloads on the bus. Option B keeps each message ≤ KBs and lets consumers subscribe by type without downloading data they will ignore.

A third option C — publish a thin envelope on `pokt.block.{H}` containing an S3/HTTP URL back to the FilePlugin archive — was rejected: the indexer must not reach out of NATS to consume canonical bytes (out-of-scope per project intent; couples live ingest to bucket availability).

## Decision

**Adopt option B**. The sidecar parses the per-block FilePlugin files in-process and publishes:

```
pokt.block.{H}                  // 1 msg = block envelope: header + hash + tx_count + event_count + chain_id
pokt.tx.{H}.{idx}               // 1 msg = 1 tx (raw bytes + tx_result section)
pokt.events.{eventType}.{H}     // 1 msg = 1 event (already defined in internal/nats/subjects.go)
pokt.kv.{store}.{H}             // 1 msg = 1 StoreKVPair (already defined)
```

Rules:

1. **No NATS message may exceed 256 KiB soft cap.** Server `max_payload` stays at the JetStream default (1 MiB) as a hard ceiling. Sidecar logs at WARN above the soft cap, refuses to publish above the hard cap.
2. **No reference to external storage in any NATS payload.** No S3/HTTP URLs, no object-store handles, no file paths. The indexer's live ingest path depends only on what flows through NATS.
3. **Block envelope is metadata only.** `pokt.block.{H}` carries chain metadata sufficient to compose, sort, and verify completeness — never the block's transactional body.
4. **Per-message determinism**: every published message has a `Nats-Msg-Id` derived from `(subject + height + intra-block-index)` so JetStream dedup is exact across sidecar restarts.

### What NATS is NOT used for

- Bootstrap from height N: bootstrap consumes the FilePlugin archive directly (see [ADR-023](ADR-023-live-vs-bootstrap-boundary.md)). The same sidecar binary can be pointed at a local replicated archive; the message shape on NATS is identical.
- Large blob delivery: if a future use case truly needs a blob > 256 KiB on the bus, write it to JetStream Object Store and reference by `<bucket, key>` (not external URL) — but the current scope does not contain such a use case.

## Consequences

### Positive

- Each message is sub-KB to low-KB. Consumers can subscribe to type slices and ignore everything else without paying bandwidth or parse cost.
- JetStream stream sizing and replication behave predictably; no pathological large messages mid-stream.
- Sidecar restart is transparent: dedup via `Nats-Msg-Id` keeps the indexer effectively-once even if the sidecar re-publishes after crash.
- Bootstrap and live ingest share the same consumer code path — the only difference is who produces NATS messages (sidecar vs archive replayer).

### Negative

- More messages on the bus. Mitigated by NATS being designed for millions msg/s and by per-consumer batching on the write side (see [ADR-024](ADR-024-consumer-batching.md)).
- Sidecar must parse the protobuf in-process before publishing. Bounded CPU cost; not a hotpath.
- Per-block ordering across subjects requires explicit coordination (see [ADR-025](ADR-025-indexer-coordination.md)).

## References

- [ADR-003](ADR-003-fileplugin-and-sidecar.md) — FilePlugin + sidecar architecture
- [ADR-004](ADR-004-nats-jetstream-over-kafka.md) — NATS choice
- [ADR-007](ADR-007-per-module-consumers.md) — per-module consumers
- [ADR-023](ADR-023-live-vs-bootstrap-boundary.md) — live ingest vs bootstrap boundary
- [ADR-024](ADR-024-consumer-batching.md) — batching discipline
- [ADR-025](ADR-025-indexer-coordination.md) — indexer coordination
- `internal/nats/subjects.go` — canonical subject constructors
