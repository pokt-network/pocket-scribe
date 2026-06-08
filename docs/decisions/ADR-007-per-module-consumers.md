# ADR-007: Per-module consumers with NATS queue groups

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

After the chain data lands in NATS JetStream, we need to process it into Postgres. Two extremes were considered:

- **Single mega-consumer** (Pocketdex pattern with `Promise.all` dispatch): one process processes everything per block atomically.
- **Per-event consumer**: one consumer per event type.

We want:
- Independent scaling per module (some are heavier than others).
- Independent deployment (bug fix in tokenomics consumer shouldn't redeploy supplier consumer).
- Failure isolation (consumer crash affects only its module).
- Reasonable simplicity.

## Decision

**One consumer per module** (supplier, application, gateway, service, session, tokenomics, bank, authz, validator). Each consumer is a separate process (or pod) running `ps consumer <module>`. NATS queue groups enable horizontal scaling within a module.

## Consequences

### Positive

- **Independent deploy** per module — rebuild + redeploy the supplier consumer without touching others.
- **Independent scaling** — heavy modules (tokenomics?) can have 5 pods; light ones 1.
- **Failure isolation** — supplier consumer panic doesn't stop application processing.
- **Subject-filtered subscriptions** — supplier consumer subscribes to `pokt.kv.supplier.>` only; doesn't see noise.
- **Per-module observability** — `pocketscribe_consumer_processed_blocks_total{consumer="supplier"}` gives module-level metrics.
- **Per-module reindex** — `ps replay --module=supplier --from=X --to=Y` reindexes just that module.

### Negative

- **More processes to monitor** — 9 modules × N pods each. Mitigated by good observability (Grafana dashboards per consumer).
- **No cross-module atomicity in a single block** — supplier consumer may write block H rows before application consumer does. Acceptable because append-only schema is commutative; the `safe_height` view provides a barrier for cross-entity-consistent queries.
- **Some cross-module data duplication** — both supplier and tokenomics consumers see the same `EventClaimSettled` event (one for the supplier perspective, one for the mint/burn ops). Each writes its own perspective.

### Neutral

- Resource overhead: each pod ~256 MB. 9 modules × 3 pods average = 27 pods × 256 MB = 7 GB. Modest.

## Alternatives considered

### Option A: Mega-consumer
- Pro: cross-module atomicity per block.
- Con: single point of failure, single scaling unit, single deploy unit.
- Con: hard to reason about (89 entities in one handler).
- **Rejected because**: this is the Pocketdex pattern that caused the rewrite.

### Option B: Per-event consumer
- Pro: ultimate granularity.
- Con: most events affect multiple entity types; per-event consumers would coordinate across modules.
- Con: 30+ consumers to monitor.
- **Rejected because**: granularity not justified; module-level is the right boundary.

### Option C: Per-shard consumer (partition by entity hash)
- Pro: linear horizontal scaling within a module.
- Con: complexity (subject naming, repartition during scale events).
- **Deferred**: start with queue groups (simpler); upgrade to partitioning when a single module's throughput becomes the bottleneck.

## Implementation notes

NATS queue group:
```
Consumer name: supplier-indexer (durable)
FilterSubject: pokt.kv.supplier.>
Replicas (pods): N
NATS load-balances messages across the N pods.
```

Per-pod processing:
```go
for msg := range subscribe(...) {
    tx, _ := db.Begin()
    handler.Process(tx, msg)               // writes to supplier_history
    db.UpdateProcessedHeights(tx, "supplier", msg.Height)
    tx.Commit()
    msg.Ack()  // AFTER commit
}
```

Scaling decisions:
- Default: 1 pod per module.
- Scale up when `pocketscribe_consumer_lag_blocks{consumer=X} > 100` sustained.
- Scale down when consistently caught up + low CPU.

CLI:
```
ps consumer supplier           # run one consumer
ps indexer                     # run all enabled consumers in one process (dev convenience)
```

`ps indexer` is the dev/test convenience; production runs separate pods per module.

## References

- Full session transcript: Topic 6, Topic 11.
- ADR-004 (NATS JetStream) — the bus that enables queue groups.
- ADR-005 (append-only pure) — what makes order-independent queue groups safe.
- `docs/architecture/07-ha-scaling.md` — full production topology.
