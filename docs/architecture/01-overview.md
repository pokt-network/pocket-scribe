# 01 — Architecture Overview

## What PocketScribe is

PocketScribe is a **producer** of indexed Pocket Network data. It reads the chain (via the official Cosmos SDK ABCI StreamingService), persists authoritative entity snapshots and event streams to PostgreSQL+TimescaleDB, materializes time-bucketed aggregates with gap-aware sealing, and exposes a stable schema for downstream consumers.

It is explicitly **not**:
- A GraphQL/REST server (those are downstream: Hasura, PostgREST).
- A real-time push gateway (downstream: NATS WebSocket bridge).
- A frontend explorer (separate project).
- A wallet, mempool tracker, or signing tool.

## High-level data flow

```
                ┌─────────────────────────────────────────────┐
                │  poktroll archive node(s) — no pruning      │
                │  Cosmos SDK v0.53.0 + CometBFT fork         │
                │                                             │
                │  app.toml [streaming.abci] plugin="file"    │
                │  keys=["supplier","application",...]        │
                │                                             │
                │  RegisterStreamingServices()                │
                └──────────────────┬──────────────────────────┘
                                   │
                                   ▼ writes TWO files per block
                   /var/lib/poktroll/streaming/
                       block-{H}-meta   (FinalizeBlock req/res)
                       block-{H}-data   (StoreKVPair changes)
                                   │
                                   ▼ tail (inotify)
                ┌─────────────────────────────────────────────┐
                │  ps fileplugin  (Go, ~300 LoC)              │
                │  - reads finalized block files              │
                │  - decodes minimally for fan-out subjects   │
                │  - publishes to NATS with Nats-Msg-Id       │
                │  - deletes files after publish + ack +      │
                │    safety_window (e.g. 100 blocks)          │
                └──────────────────┬──────────────────────────┘
                                   │
                                   ▼
            ┌──────────────────────────────────────────────────┐
            │  NATS JetStream cluster (3 replicas, file mode)  │
            │                                                  │
            │  Stream: POKT_CHAIN                              │
            │  Subjects: pokt.block.{height}                   │
            │            pokt.kv.{store}.{height}              │
            │            pokt.events.{type}.{height}           │
            │  Retention: 30 days                              │
            │  Dedup window: 24h on Nats-Msg-Id                │
            └──────────────────┬───────────────────────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
  ┌─────────────┐      ┌─────────────┐         ┌─────────────┐
  │ supplier-   │      │application- │  ...    │tokenomics-  │
  │ consumer    │      │consumer     │         │consumer     │
  │             │      │             │         │             │
  │ durable     │      │ durable     │         │ durable     │
  │ pull-based  │      │ pull-based  │         │ pull-based  │
  │ ack-after-  │      │ ack-after-  │         │ ack-after-  │
  │ commit      │      │ commit      │         │ commit      │
  └──────┬──────┘      └──────┬──────┘         └──────┬──────┘
         │                    │                       │
         └────────────────────┼───────────────────────┘
                              ▼
            ┌────────────────────────────────────────────┐
            │  PostgreSQL 18 + TimescaleDB OSS           │
            │                                            │
            │  Tables (relational):                      │
            │   - block, processed_heights               │
            │   - consumer_consolidation                 │
            │   - aggregate_registry, bucket_seal        │
            │   - param_history (SCD2)                   │
            │                                            │
            │  Tables (append-only history):             │
            │   - supplier_history, application_history  │
            │   - gateway_history, service_history       │
            │   - session_history, validator_history     │
            │                                            │
            │  Hypertables (events, time-series):        │
            │   - event_claim_settled                    │
            │   - event_proof_updated                    │
            │   - mint_burn_op                           │
            │   - event_supplier_slashed                 │
            │                                            │
            │  Continuous aggregates:                    │
            │   - rewards_hourly, rewards_daily          │
            │   - relays_hourly, claims_hourly           │
            │   - ... (registered in aggregate_registry) │
            └────────────────────────────────────────────┘
                              ▲
                              │
            ┌─────────────────┴──────────────────────────┐
            │  Reconciler (cron, every 10 min)           │
            │  - bulk gRPC ListSuppliers etc. at height  │
            │  - compare with indexed state              │
            │  - alert on drift; auto-heal by snapshot   │
            └────────────────────────────────────────────┘

  ── Downstream consumers (NOT part of PocketScribe scope) ──

            ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐
            │ Hasura       │  │ PostgREST    │  │ NATS WS bridge     │
            │ GraphQL      │  │ REST+OpenAPI │  │ realtime push      │
            └──────────────┘  └──────────────┘  └────────────────────┘
```

## Component summary

All shipped as subcommands of a single `ps` binary (`cmd/ps`).

| Component | Subcommand | LoC est. | Stateless? | Replicable? |
|---|---|---|---|---|
| Sidecar publisher | `ps fileplugin` | ~300 | Yes (cursor on disk) | One per archive node |
| Indexer consumer | `ps consumer <module>` | ~400/module | Yes (cursor in DB) | Yes, queue group |
| Reconciler | `ps reconciler` | ~300 | Yes | No (singleton, cron) |
| Sealing loop | `ps sealing` | ~200 | Yes | No (singleton) |
| Migrations | `ps migrate up/down/status` | ~50 | N/A | One-shot |
| Health check | `ps doctor` | ~150 | Yes | One-shot |

## Key architectural decisions (the short list)

Each is detailed in its own ADR in `docs/decisions/`:

1. **ADR-001** Go over Rust → leverages poktroll ecosystem, native protos, hiring pool.
2. **ADR-002** PostgreSQL + TimescaleDB OSS over ClickHouse → operational sovereignty, operational pain with OSS analytics-DB replication.
3. **ADR-003** Official FilePlugin + Go sidecar over custom in-process plugin → zero risk to consensus path.
4. **ADR-004** NATS JetStream over Kafka/Redpanda → single binary, simpler operations, strong dedup, OSS HA.
5. **ADR-005** Append-only pure (no `valid_to_height`) → commutative, late-arrival safe.
6. **ADR-006** Chain as source of truth (snapshots from KV writes) over event-derived state → eliminates drift.
7. **ADR-007** Per-module consumers with queue groups → independent scaling, no monolith.
8. **ADR-008** Versioned proto decoders with height-based router → handles upgrades over chain lifetime.
9. **ADR-009** Bucket sealing with `consumer_consolidation` → gap-aware materialization.
10. **ADR-010** `(block_height, block_time)` invariant → time queries via height bounds.
11. **ADR-011** Downstream APIs not in scope → Hasura/PostgREST/NATS-bridge are separate.
12. **ADR-012** 5-layer testing strategy → unit, component, golden, integration, E2E.

## Read in this order

If new to the project:
1. `01-overview.md` (you are here)
2. `02-ingestion.md` — how data enters
3. `03-data-model.md` — the schema philosophy
4. `04-aggregates.md` — sealing and continuous aggregates
5. `05-versioning.md` — multi-version protobuf handling
6. `07-ha-scaling.md` — production topology
7. `08-reconciliation.md` — drift detection
8. `09-backfill.md` — historical replay
9. `10-testing.md` — how we know it works
10. `06-apis.md` — the (not-our-scope) consumer story
