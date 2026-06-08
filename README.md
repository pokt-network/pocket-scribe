# PocketScribe 🪶

> The faithful chronicler of Pocket Network.

A Go-native indexer for Pocket Network's Shannon protocol. Stream-first ingestion via ABCI FilePlugin, append-only state snapshots with chain-as-source-of-truth, version-aware decoders, and bucket-sealed analytics. Built for owned infrastructure. No cloud lock-in.

## Status

**🚧 Pre-spike — schema foundation complete.** The full chain data model (244 tables covering poktroll + cosmos-sdk core) is generated and validated end-to-end. No Go runtime code yet — the first spike (see [ROADMAP.md](./ROADMAP.md)) wires consumers + decoders on top of this foundation.

**Visual overview**: [`docs/architecture/00-system-flow.md`](./docs/architecture/00-system-flow.md) — 4 mermaid diagrams covering live ingestion, schema generation pipeline, decoder routing, and archeology substrate.

What exists today: skills + schema + archeology. See [STATUS.md](./STATUS.md) for the precise breakdown.

## Why another indexer

| Past pain (legacy indexers) | PocketScribe answer |
|---|---|
| Single-thread / per-block tx (Node.js) | Native Go, per-module consumers, parallel processing |
| Math bugs causing accumulated drift | Snapshots from chain — never computed locally |
| Reindex = days of downtime | Append-only history + per-module reindex in minutes |
| Silent indexer/chain drift | Reconciler with bulk gRPC comparison + auto-heal |
| Replication pain on legacy analytics DBs | Postgres + TimescaleDB OSS, single DB |
| Subscriptions tied to write primary | NATS WebSocket bridge; queries hit replicas |
| Schema changes = full rewrite | Versioned proto decoders per upgrade height |

## Stack (latest stable always)

- **Language**: Go 1.26+
- **Chain**: poktroll (Cosmos SDK v0.53.0 + CometBFT fork)
- **Ingestion**: official Cosmos SDK ABCI `FilePlugin`
- **Bus**: NATS JetStream 2.10+ (3-replica cluster, file storage)
- **Storage**: PostgreSQL 18+ with TimescaleDB OSS 2.18+
- **Migrations**: goose
- **Codegen**: buf (protos), sqlc (queries)
- **DB driver**: pgx v5
- **Local dev**: Tilt + kind/k3d
- **Testing**: testcontainers-go + sebdah/goldie/v2
- **CLI**: cobra + viper, single `ps` binary with subcommands

## Architecture in 60 seconds

```
poktroll archive node (no-prune)
   │  official Cosmos SDK FilePlugin
   ▼
/var/lib/poktroll/streaming/block-{H}-data + block-{H}-meta
   │  ps fileplugin (sidecar)
   ▼
NATS JetStream (3 replicas, dedup by Msg-Id, file storage)
   │
   ├──► ps consumer supplier ──┐
   ├──► ps consumer application ┤
   ├──► ps consumer gateway     ┤──► PostgreSQL 18 + TimescaleDB
   ├──► ps consumer tokenomics  ┘    ├─ entity history (append-only)
   │                                  └─ event hypertables (time-series)
   └──► ps sealing ─────────────► continuous aggregates
                                   (bucket-sealed, gap-aware)

   ps reconciler ───► periodic bulk gRPC list ──► drift detection + auto-heal

Downstream (NOT part of PocketScribe):
   ├──► Hasura          → GraphQL
   ├──► PostgREST       → REST + OpenAPI
   └──► NATS WS bridge  → real-time push
```

Full design in [`docs/architecture/`](./docs/architecture/).

## CLI overview

Single binary `ps`, cobra subcommands:

```bash
# Long-running services
ps fileplugin                  # sidecar: tails FilePlugin dir → NATS
ps consumer <module>           # run one module consumer
ps indexer                     # run all enabled consumers in one process
ps reconciler                  # periodic drift detection
ps sealing                     # bucket sealing loop

# Admin
ps migrate up | down | status
ps inspect streams | cursors | seals
ps replay --module=X --from=H1 --to=H2
ps backfill --from-genesis
ps reconcile --module=X
ps doctor                      # health check: DB, NATS, node
ps version
```

Full CLI in [`docs/architecture/02-ingestion.md`](./docs/architecture/02-ingestion.md) and [`CLAUDE.md`](./CLAUDE.md).

## Quickstart (Tilt-based local dev)

When code exists:

```bash
# Clone
git clone https://github.com/pokt-network/pocketscribe
cd pocketscribe

# Install deps (Go, Tilt, kind, kubectl, helm)
make install-deps

# Spin local cluster
make cluster-up                  # creates kind cluster

# Bring up full local stack via Tilt (poktroll + NATS + Postgres + ps)
tilt up                          # opens UI at http://localhost:10350

# Tail logs, hot-reload on file save, all in the Tilt UI.

# Tear down
tilt down
make cluster-down
```


## Documentation

- [`CLAUDE.md`](./CLAUDE.md) — operating rules for Claude Code working on this repo (also valuable for humans)
- [`docs/architecture/`](./docs/architecture/) — full system design (10 documents)
- [`docs/decisions/`](./docs/decisions/) — ADRs for every major call
- [`docs/operations/`](./docs/operations/) — deployment, monitoring, runbooks
- [`docs/research/`](./docs/research/) — focused technical research
- [`ROADMAP.md`](./ROADMAP.md) — phased plan from spike to production
- [`CONTRIBUTING.md`](./CONTRIBUTING.md) — how to add module / aggregate / version

## Hard rules (short version)

1. Every row carries `(block_height, block_time)` from the chain header. **Never** indexer write time.
2. State entities are append-only. No `UPDATE`. No `valid_to_*`. Ranges via `LEAD()`.
3. The chain is the source of truth. We never compute state — we mirror it.
4. Ack NATS messages **after** Postgres commit. Never before.
5. No cloud-managed services. No Node.js framework indexers.
6. Test-driven by default. DRY across the codebase.
7. Local dev runs in Kubernetes (kind/k3d) via Tilt — same as production.

Full rules + rationale: [`CLAUDE.md`](./CLAUDE.md).

## License

TBD (likely MIT or Apache-2.0 to match the broader Pocket ecosystem).

## Credits

