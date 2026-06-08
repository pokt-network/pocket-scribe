# ADR-004: NATS JetStream over Kafka / Redpanda

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

The ingestion pipeline needs a durable message bus between the FilePlugin sidecar (producer) and the consumer pool (downstream). Requirements:

- **Self-hosted with OSS HA** (no Confluent Cloud, no MSK).
- **Dedup at the bus** (since 2 archive nodes publish the same data).
- **Per-consumer durable subscriptions** with explicit ack.
- **Operationally simple** for a small team.
- **At-least-once delivery** with effective-once via consumer-side idempotency.

Three options considered: Apache Kafka, Redpanda (Kafka-compatible Go binary), NATS JetStream.

## Decision

Use **NATS JetStream** (3-replica cluster, file storage) as the message bus.

## Consequences

### Positive

- **Single Go binary**, no JVM, no Zookeeper, no KRaft setup.
- **OSS HA via Raft consensus** — 3 replicas tolerate 1-node failure.
- **Native dedup** via `Nats-Msg-Id` header — perfect for HA active-active producers.
- **Durable pull consumers** with explicit ack + queue groups for horizontal scaling.
- **Subject hierarchies** (`pokt.kv.supplier.123`) with wildcard filtering for fan-out.
- **Real-time aggregation friendly** — same bus serves the WebSocket bridge for downstream realtime.
- **Tooling**: `nats` CLI is excellent for inspection (`nats consumer report`, `nats stream ls`).
- **Smaller resource footprint** than Kafka (no JVM heap).

### Negative

- **Smaller ecosystem** than Kafka. Fewer Kafka-style connectors.
- **JetStream is younger** than Kafka — fewer war stories at extreme scale (PocketScribe is well below extreme scale).
- **No native log compaction** (Kafka has it; not needed here since we have explicit retention).

### Neutral

- Both have client libraries in Go (NATS official client is excellent).
- Both support exactly-once semantics with idempotent consumers.

## Alternatives considered

### Option A: Apache Kafka
- Pro: industry standard, massive ecosystem.
- Con: JVM, Zookeeper or KRaft setup, larger operational footprint.
- Con: dedup requires custom logic (Kafka has idempotent producer but per-producer-session, not bus-level by Msg-Id).
- **Rejected because**: operational overhead not justified for our scale and team size.

### Option B: Redpanda (Kafka-compatible)
- Pro: single Go binary, no JVM, Kafka-API-compatible.
- Pro: faster than Kafka, lower latency.
- Con: dedup story same as Kafka (per-producer).
- Con: smaller community than NATS for streaming-data patterns.
- **Considered seriously**; we may revisit if a future Kafka-ecosystem tool is critical.

### Option C: Stream from FilePlugin → MinIO/S3 → consumers pull
- Pro: durable by design.
- Con: high latency for live (poll-based pulls).
- Con: complex partitioning to scale.
- **Rejected because**: latency profile wrong for live ingestion.

## Implementation notes

Stream config:
```
Stream: POKT_CHAIN
  Subjects: pokt.>
  Storage: file
  Retention: limits
  MaxAge: 30d
  Replicas: 3
  Discard: old
  Duplicate window: 24h         # dedup by Nats-Msg-Id
  MaxConsumers: -1
```

Consumer config:
```
Consumer: <module>-indexer
  Durable: true
  AckPolicy: explicit
  AckWait: 30s
  MaxAckPending: 1            # serial per consumer; queue group for parallelism
  DeliverPolicy: from-sequence (resume from last ack)
  FilterSubject: pokt.kv.<module>.>
```

Subject naming (canonical, in `internal/nats/subjects.go`):
- `pokt.block.{height}` — full block payload.
- `pokt.kv.{store}.{height}` — per-store KV writes.
- `pokt.events.{event_type}.{height}` — per-event-type fan-out.

Future option: partition subjects by entity hash for strict ordering per entity (`pokt.kv.supplier.{partition}.{height}`). Not needed initially because append-only schema is commutative.

## References

- Full session transcript: Topic 4 (cloud + ClickHouse rejection set the stage), Topic 11 (HA topology).
- ADR-003 (FilePlugin + sidecar) — the producer.
- ADR-007 (per-module consumers) — the consumer pool.
- CLAUDE.md "Stack commitments" + banned list (Kafka managed services).
