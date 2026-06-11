# 07 — High Availability & Scaling

## Production topology

```
                       ┌──────────────────────┐  ┌──────────────────────┐
                       │ poktroll archive #1  │  │ poktroll archive #2  │
                       │ FilePlugin enabled   │  │ FilePlugin enabled   │
                       └──────────┬───────────┘  └──────────┬───────────┘
                                  │                          │
                                  ▼                          ▼
                       ┌──────────────────────┐  ┌──────────────────────┐
                       │ ps fileplugin        │  │ ps fileplugin        │
                       │ (sidecar)            │  │ (sidecar)            │
                       └──────────┬───────────┘  └──────────┬───────────┘
                                  │                          │
                                  └───────────┬──────────────┘
                                              ▼ (Nats-Msg-Id dedup)
                          ┌──────────────────────────────────────────┐
                          │  NATS JetStream 3-replica cluster        │
                          │  file storage, 30d retention             │
                          │  24h dedup window                        │
                          └──────────────────────────────────────────┘
                                              │
              ┌───────────────────────────────┼─────────────────────────┐
              ▼                               ▼                         ▼
       ┌────────────┐                 ┌────────────┐            ┌────────────┐
       │ ps consumer│                 │ ps consumer│   ...      │ ps consumer│
       │ supplier x3│                 │ application│            │ tokenomics │
       │ (queue grp)│                 │ x3         │            │ x2         │
       └─────┬──────┘                 └─────┬──────┘            └─────┬──────┘
             │                              │                          │
             └──────────────────────────────┼──────────────────────────┘
                                            ▼
                          ┌──────────────────────────────────────────┐
                          │  PostgreSQL 18 + TimescaleDB OSS         │
                          │  ┌────────────────────────────────┐      │
                          │  │ Primary (writes + LISTEN/NOTIFY) │   │
                          │  └─────────────┬──────────────────┘      │
                          │                │ streaming replication    │
                          │   ┌────────────┴──────────┐               │
                          │   ▼                       ▼               │
                          │ Replica #1            Replica #2          │
                          │ (Hasura queries)      (analytics queries) │
                          └──────────────────────────────────────────┘
                                            ▲
                                            │
                          ┌────────────────────────────────────────┐
                          │  ps reconciler (singleton, cron)       │
                          │  bulk gRPC list → drift check → heal   │
                          └────────────────────────────────────────┘
```

## HA layers

### 1. Archive nodes — active-active

Run 2+ poktroll archive nodes. Each independently:
- Syncs with mainnet peers.
- Runs FilePlugin → local sidecar.
- Sidecar publishes to NATS with `Nats-Msg-Id="block-{H}"`.

**Dedup at the bus**: JetStream rejects duplicates within the 24h dedup window. Both nodes publishing the same block = one stored.

**Failover**: zero manual action. If node A goes down, node B continues. When A returns, it catches up and starts publishing again — dedup eats the catchup duplicates.

**Sizing**: 8 CPU / 16 GB / 1 TB SSD per archive node (Shannon grows ~50 GB/month).

### 2. NATS JetStream — 3-replica cluster

Three NATS nodes in a Raft cluster.

```
nats-1 (leader)     ↔     nats-2 (follower)
       ↘                  ↙
              nats-3 (follower)
```

- File storage (not memory).
- Replication factor 3.
- Tolerates 1-node failure.
- Auto-elects new leader on failure.

**Sizing**: 4 CPU / 8 GB / 500 GB SSD per node (30-day retention).

### 3. Consumers — queue groups

For each consumer subject (`pokt.kv.supplier.>`, etc.), run N consumer pods in a **NATS queue group**. NATS load-balances messages across pods.

```
Queue group: supplier-indexer
  - ps-consumer-supplier-0
  - ps-consumer-supplier-1
  - ps-consumer-supplier-2
```

If pod 0 crashes, NATS redistributes its in-flight messages to pods 1 and 2 (after `AckWait` timeout).

**Order preservation**:
- Default: queue group does NOT preserve subject order. Safe because of [ADR-005 append-only pure](../decisions/ADR-005-append-only-pure.md).
- If strict order needed for a specific module (rare): partition the subject by entity hash (`pokt.kv.supplier.{0..15}.{height}`) and run one consumer per partition.

**Sizing**: each consumer pod ~0.5 CPU / 256 MB. Scale horizontally based on lag metric.

### 4. PostgreSQL — primary + read replicas

- 1 primary (writes + LISTEN/NOTIFY).
- 2+ read replicas (streaming replication).
- Hasura `read_replicas` config routes queries to replicas, mutations + subscriptions to primary.
- PostgREST: configure with replica connection string for `db-uri`.

**Failover**: managed manually or via Patroni / Stolon (Postgres HA tools). Out of scope for PocketScribe; deployment-specific.

**Backups**: pgBackRest with PITR. Stored on S3-compatible (MinIO).

**Sizing**:
| Component | CPU | RAM | Disk |
|---|---|---|---|
| Primary | 16 | 64 GB | 2 TB SSD (NVMe preferred) |
| Replica | 8 | 32 GB | 2 TB SSD |

Plan for 2-3 years of Shannon data growth.

### 5. Reconciler — singleton

The reconciler is a **singleton** — one instance running at a time. It's stateless (state is in `consumer_consolidation` + `*_history`) so restart is cheap.

Run as a Kubernetes `Deployment` with `replicas: 1`. If you need HA reconciler, add a leader election layer (chosen via a Postgres advisory lock).

### 6. Sealing loop — singleton

Same as reconciler. Singleton, Postgres-state-driven, restart-safe.

## Scaling levers (in order of cost)

### Horizontal scaling per consumer

Cheapest. Add pods to the queue group. NATS load-balances. Limited only by Postgres write throughput (~50k writes/sec on a single primary).

```yaml
# Kubernetes Deployment
spec:
  replicas: 5    # was 3
```

### Vertical scaling Postgres primary

Add CPU/RAM/IOPS. Often the bottleneck before consumers. Modern NVMe + 64 GB RAM Postgres handles ~50k writes/sec.

### Read replica fan-out

For read-heavy workloads (heavy GraphQL traffic, dashboards), add 1-2 more replicas. Each replica eats ~1 vCPU on the primary for replication.

### Partition NATS subjects

If a single consumer's processing rate can't keep up with publish rate at horizontal scale, partition by entity hash:

```
pokt.kv.supplier.{0..15}.{height}
```

16 partitions × 1 consumer each = 16-way parallel with order preserved per entity.

### Citus (Postgres sharding)

If single-node Postgres hits a wall (>5 years of data, >100k writes/sec), shard with [Citus](https://www.citusdata.com/) (OSS). Out of scope for MVP but on the runway.

## Failure scenarios and recovery

| Failure | Detection | Recovery |
|---|---|---|
| One archive node crashes | Prometheus alert (`up{job="poktroll"} == 0`) | Other archive continues. Restart failed node; it catches up from peers, dedup eats duplicates. |
| Sidecar OOM | Pod restart by K8s | Resume from cursor on disk. <30s downtime. |
| All sidecars down | All-node alert | Files accumulate on disk. Disk hits threshold → node halts. **Restart sidecars first.** |
| NATS leader dies | Raft re-elects | <5s leader change; consumers reconnect. |
| All NATS nodes down (cluster-wide) | All-down alert | Sidecars stop publishing; files accumulate. Restart NATS; sidecars resume. Possible loss if disk fills first → node halt → no actual chain loss. |
| One consumer pod OOMs | Pod restart | NATS redistributes its in-flight messages. <30s. |
| All consumers for module X down | Lag alert (`consumer_lag_blocks > 1000`) | Restart pods. Lag drains over minutes. |
| Postgres primary fails | Connection errors | Failover to replica (manual or Patroni). Re-point Hasura/consumers to new primary. |
| Postgres data corruption | Replication lag alarms; integrity checks | Restore from pgBackRest PITR. Reindex affected ranges. |
| Reconciler crashes | Alert: `reconciler_last_run_timestamp_seconds` stale | Restart pod. State preserved. |
| Sealing loop crashes | Alert: `sealing_loop_iterations_total` not incrementing | Restart pod. State preserved. |
| Operator drops the wrong table | Backup restore | pgBackRest + reindex from NATS/archive. |
| poktroll mainnet halts | External alert | Wait for governance / chain restart. Reconciler stays paused until tip moves. |

## Cost estimate (production, monthly)

Rough USD/month for a self-hosted deployment on commodity cloud or bare metal:

| Component | Spec | Cost |
|---|---|---|
| 2× archive nodes | 8 CPU / 16 GB / 1 TB NVMe | $200 |
| 3× NATS cluster | 4 CPU / 8 GB / 500 GB | $180 |
| 1× Postgres primary | 16 CPU / 64 GB / 2 TB NVMe | $250 |
| 2× Postgres replica | 8 CPU / 32 GB / 2 TB NVMe | $250 |
| N× consumers | varies; ~20 pods | $50 |
| Observability stack | Prometheus + Loki + Grafana | $80 |
| Misc (load balancer, monitoring, backups) | — | $100 |
| **Total** | | **~$1,100** |

vs. cloud-managed equivalent: easily $5,000-15,000+ for same scale. Self-hosting pays for itself.

## See also

- ADR-007 (per-module consumers with queue groups) — rationale.
- `docs/operations/deployment.md` — actual K8s manifests / Helm values.
- `docs/operations/disaster-recovery.md` — detailed runbooks.
